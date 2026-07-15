"""集合竞价跳空策略候选生成与日线近似回测。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from typing import Literal

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

from rquant.data_contracts import EXCHANGE_TIMEZONE
from rquant.paper import (
    PaperPosition,
    PaperRiskPlan,
    PaperTradeConfig,
    adjust_open_position_price_basis,
    check_position_exit,
    mark_position_to_quote,
    open_position_from_signal,
)
from rquant.pit_visibility import (
    VisibilityQueryScope,
    query_visible_rows,
    visible_sources_at,
)
from rquant.signal_provenance import (
    AUCTION_GAP_MINUTE_STRATEGY,
    SignalProvenance,
    build_signal_factors,
    persist_position_with_provenance,
)
from rquant.state.derive import (
    ListingPriceLimitFact,
    _classify_board,
    _historical_limit_pct,
    _price_limit_eligibility,
    _round_half_up,
)
from rquant.stock_features import build_intraday_relative_volume_features
from rquant.storage.duckdb import DuckDBStore
from rquant.topn_selection import FeatureScoreTerm, score_feature_terms

GapMode = Literal["close", "strict_high"]
StFilterMode = Literal["case_insensitive", "literal_lower", "none"]
ExitMode = Literal["next_open"]
AuctionGapMinuteEntryMode = Literal["vwap_push"]
HoldPolicy = Literal["t1", "seal_hold"]

_AUCTION_GAP_MINIMUM_COLUMNS = ["signal_date", "ts_code", "name"]

# factor_confirm 对照实验（2026-07-02 归因终审已判死该策略线，此处只做低成本
# 死刑复核）：不引入新因子，评分项全部来自 AUCTION_GAP_V1 既有键名（满分 100）。
AUCTION_GAP_B_V1_SCORE_TERMS: tuple[FeatureScoreTerm, ...] = (
    FeatureScoreTerm(
        name="auction_vol_ratio_5d", group="auction", weight=20.0,
        transform="log_ratio", cap=5.0, skip_if_missing=True,
    ),
    FeatureScoreTerm(
        name="gap_pct_close", group="auction", weight=20.0,
        transform="linear", low=0.0, high=6.0, skip_if_missing=True,
    ),
    FeatureScoreTerm(
        name="vwap_position", group="minute", weight=15.0,
        transform="linear", low=1.0, high=1.02, skip_if_missing=True,
    ),
    FeatureScoreTerm(
        name="limit_progress", group="minute", weight=20.0,
        transform="linear", low=0.2, high=1.0, skip_if_missing=True,
    ),
    FeatureScoreTerm(
        name="support_ok", group="minute", weight=10.0,
        transform="linear", low=0.0, high=1.0, skip_if_missing=True,
    ),
    FeatureScoreTerm(
        name="rel_amount_same_minute_20d", group="minute", weight=10.0,
        transform="log_ratio", cap=5.0, skip_if_missing=True,
    ),
    FeatureScoreTerm(
        name="amount_accel_5m", group="minute", weight=5.0,
        transform="log_ratio", cap=5.0, skip_if_missing=True,
    ),
)


class AuctionGapConfig(BaseModel):
    """集合竞价跳空策略配置。"""

    model_config = ConfigDict(frozen=True)

    start_date: str
    end_date: str
    gap_mode: GapMode = "close"
    min_auction_vol_ratio_5d: float = 0.15
    max_auction_vol_ratio_5d: float = 5.0
    st_filter: StFilterMode = "case_insensitive"
    decision_time: time = time(9, 27)
    exit_mode: ExitMode = "next_open"
    daily_vol_unit_factor: float = 100.0
    price_tol: float = 0.01
    require_next_day: bool = True


class AuctionGapSummary(BaseModel):
    """集合竞价跳空策略回测摘要。"""

    model_config = ConfigDict(frozen=True)

    trades_count: int
    hit_limit_up_rate_pct: float | None = None
    next_open_win_rate_pct: float | None = None
    next_open_mean_ret_pct: float | None = None
    next_open_median_ret_pct: float | None = None
    intraday_high_mean_ret_pct: float | None = None
    intraday_max_drawdown_mean_pct: float | None = None


class AuctionGapMinuteReplayConfig(BaseModel):
    """集合竞价候选 + 分钟级 B/S 回放配置。"""

    model_config = ConfigDict(frozen=True)

    start_date: str
    end_date: str
    gap_mode: GapMode = "close"
    min_auction_vol_ratio_5d: float = 0.15
    max_auction_vol_ratio_5d: float = 5.0
    st_filter: StFilterMode = "case_insensitive"
    freq: str = "1min"
    entry_mode: AuctionGapMinuteEntryMode = "vwap_push"
    entry_start_time: time = time(9, 31)
    entry_pullback_tolerance_pct: float = Field(default=0.02, ge=0, lt=0.1)
    entry_vwap_buffer_pct: float = Field(default=0.0, ge=0, lt=0.05)
    min_limit_progress_pct: float = Field(default=0.2, ge=0, le=1)
    max_hold_days: int = Field(default=1, ge=1, le=10)
    next_auction_weak_gap_pct: float = Field(default=-0.01, gt=-0.2, lt=0.2)
    strong_seal_min_close_minutes: int = Field(default=3, ge=1, le=240)
    strong_seal_weak_gap_pct: float = Field(default=-0.03, gt=-0.2, lt=0.2)
    next_morning_exit_until: time = time(10, 0)
    next_morning_vwap_break_buffer_pct: float = Field(default=0.003, ge=0, lt=0.05)
    price_tol: float = 0.01
    # 封板质量驱动的条件持有期（归因报告 2026-07-02 决策项 B）：B 日收盘封住
    # 且封板质量达标（官方 open_times ≤ 阈值，可选封单/流通市值占比下限）时，
    # 持有时间上限从 max_hold_days 放宽到 seal_hold_max_days；竞价弱退/VWAP
    # 破位/止损/移动止盈等退出条件照旧生效
    seal_hold_enabled: bool = False
    seal_hold_max_days: int = Field(default=3, ge=1, le=10)
    seal_hold_max_open_times: int = Field(default=0, ge=0, le=50)
    seal_hold_min_fd_to_circ_pct: float | None = Field(default=None, ge=0)
    # 分钟 B 确认的多因子评分阈值（AUCTION_GAP_B_V1_SCORE_TERMS，None=现状不评分）。
    # 归因终审已判死该策略线，这是低成本对照实验开关
    factor_score_threshold: float | None = Field(default=None, ge=0)
    paper: PaperTradeConfig = Field(default_factory=PaperTradeConfig)

    def auction_config(self) -> AuctionGapConfig:
        return AuctionGapConfig(
            start_date=self.start_date,
            end_date=self.end_date,
            gap_mode=self.gap_mode,
            min_auction_vol_ratio_5d=self.min_auction_vol_ratio_5d,
            max_auction_vol_ratio_5d=self.max_auction_vol_ratio_5d,
            st_filter=self.st_filter,
            price_tol=self.price_tol,
        )


class AuctionGapMinuteSummary(BaseModel):
    """集合竞价候选 + 分钟 B/S 回放摘要。"""

    model_config = ConfigDict(frozen=True)

    candidates_count: int
    trades_count: int
    trigger_rate_pct: float | None = None
    mean_ret_pct: float | None = None
    median_ret_pct: float | None = None
    win_rate_pct: float | None = None
    next_auction_weak_exit_rate_pct: float | None = None


@dataclass(frozen=True)
class _AuctionWatchItem:
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
class _AuctionMinuteQuote:
    ts_code: str
    price: float
    low: float
    high: float | None


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


def _minute_quote(row: pd.Series) -> _AuctionMinuteQuote:
    return _AuctionMinuteQuote(
        ts_code=str(row["ts_code"]),
        price=float(row["close"]),
        low=float(row["low"]),
        high=float(row["high"]) if pd.notna(row["high"]) else None,
    )


def _execution_quote(row: pd.Series) -> _AuctionMinuteQuote:
    price = float(row["open"])
    return _AuctionMinuteQuote(
        ts_code=str(row["ts_code"]),
        price=price,
        low=price,
        high=price,
    )


def _close_position(
    position: PaperPosition,
    exit_time: datetime,
    exit_price: float,
    exit_reason: str,
    holding_trading_days: int,
    extra_payload: dict[str, object] | None = None,
) -> PaperPosition:
    pnl_pct = round((exit_price / position.entry_price - 1) * 100, 4)
    risk_payload = position.risk_payload or {}
    if extra_payload:
        risk_payload = {**risk_payload, **extra_payload}
    return position.model_copy(update={
        "status": "closed",
        "exit_time": exit_time,
        "exit_price": round(float(exit_price), 4),
        "exit_reason": exit_reason,
        "holding_trading_days": holding_trading_days,
        "pnl_pct": pnl_pct,
        "risk_payload": risk_payload,
    })


def _holding_days(window_dates: list[date], entry_date: date, exit_date: date) -> int:
    return sum(1 for trading_date in window_dates if entry_date < trading_date <= exit_date)


def _previous_window_date(window_dates: list[date], trading_date: date) -> date | None:
    try:
        idx = window_dates.index(trading_date)
    except ValueError:
        return None
    if idx <= 0:
        return None
    return window_dates[idx - 1]


def _merge_prior_daily_features(
    auction: pd.DataFrame,
    daily_features: pd.DataFrame,
) -> pd.DataFrame:
    """给每条竞价记录匹配信号日前最近一根日线特征。"""
    if auction.empty:
        return auction.copy()

    feature_columns = [
        "prev_trade_date",
        "pre_close",
        "pre_high",
        "avg_vol_5d",
        "prev5_count",
    ]
    if daily_features.empty:
        out = auction.copy()
        for column in feature_columns:
            out[column] = pd.NA
        return out

    parts: list[pd.DataFrame] = []
    for ts_code, auction_group in auction.groupby("ts_code", sort=False):
        prior = daily_features[daily_features["ts_code"] == ts_code].sort_values(
            "prev_trade_date"
        )
        group = auction_group.sort_values("signal_date")
        if prior.empty:
            merged = group.copy()
            for column in feature_columns:
                merged[column] = pd.NA
        else:
            merged = pd.merge_asof(
                group,
                prior.drop(columns=["ts_code"]),
                left_on="signal_date",
                right_on="prev_trade_date",
                direction="backward",
                allow_exact_matches=False,
            )
        parts.append(merged)

    return pd.concat(parts, ignore_index=True)


def _b_day_strength(
    minutes: pd.DataFrame,
    *,
    trading_date: date,
    limit_up_price: float,
    price_tol: float,
) -> dict[str, object]:
    empty_strength: dict[str, object] = {
        "b_first_limit_up_time": None,
        "b_limit_up_touch_minutes": 0,
        "b_limit_up_close_minutes": 0,
        "b_open_times": 0,
        "b_close_at_limit_up": False,
    }
    if minutes.empty or limit_up_price <= 0:
        return empty_strength
    day_minutes = minutes[
        pd.to_datetime(minutes["trade_time"]).dt.date == trading_date
    ].sort_values("trade_time")
    if day_minutes.empty:
        return empty_strength

    touch_mask = day_minutes["high"] >= limit_up_price - price_tol
    close_mask = day_minutes["close"] >= limit_up_price - price_tol
    first_time = (
        _as_datetime(day_minutes.loc[touch_mask, "trade_time"].iloc[0])
        if bool(touch_mask.any())
        else None
    )
    # 开板次数：封住状态（close 在涨停容差之上）转为跌破状态的次数，容差同涨停判定
    sealed = close_mask.astype(bool).reset_index(drop=True)
    open_times = int((sealed.shift(1, fill_value=False) & ~sealed).sum())
    return {
        "b_first_limit_up_time": first_time,
        "b_limit_up_touch_minutes": int(touch_mask.sum()),
        "b_limit_up_close_minutes": int(close_mask.sum()),
        "b_open_times": open_times,
        "b_close_at_limit_up": bool(close_mask.iloc[-1]) if len(close_mask) else False,
    }


def _query_limit_list_seal(
    store: DuckDBStore,
    ts_code: str,
    trading_date: date,
) -> pd.Series | None:
    """查官方涨停榜（limit_list_daily，limit_status='U'）的封板质量字段。"""
    df = store._conn.execute(
        """
        SELECT open_times, fd_amount, float_mv
        FROM limit_list_daily
        WHERE ts_code = ?
          AND trade_date = ?
          AND limit_status = 'U'
        """,
        [ts_code, trading_date],
    ).fetchdf()
    if df.empty:
        return None
    return df.iloc[0]


def _resolve_hold_policy(
    store: DuckDBStore,
    candidate: pd.Series,
    b_strength: dict[str, object],
    config: AuctionGapMinuteReplayConfig,
) -> tuple[HoldPolicy, int]:
    """B 日收盘后决定持有策略：封住且封板质量达标 → seal_hold，否则 t1。

    封板质量优先用官方 limit_list_daily（open_times / fd_amount / float_mv）；
    官方缺行时回退分钟推算的 b_open_times（此时封单占比条件无从验证，跳过）。
    """
    if not config.seal_hold_enabled:
        return "t1", config.max_hold_days
    if not bool(b_strength.get("b_close_at_limit_up")):
        return "t1", config.max_hold_days

    official = _query_limit_list_seal(
        store,
        str(candidate["ts_code"]),
        _as_date(candidate["signal_date"]),
    )
    if official is not None:
        open_times = (
            int(official["open_times"])
            if pd.notna(official["open_times"])
            else int(b_strength.get("b_open_times") or 0)
        )
        if open_times > config.seal_hold_max_open_times:
            return "t1", config.max_hold_days
        if config.seal_hold_min_fd_to_circ_pct is not None:
            fd_amount = official["fd_amount"]
            float_mv = official["float_mv"]
            if pd.isna(fd_amount) or pd.isna(float_mv) or float(float_mv) <= 0:
                # 封单强度无法验证时保守回 T+1
                return "t1", config.max_hold_days
            fd_to_circ_pct = float(fd_amount) / float(float_mv) * 100
            if fd_to_circ_pct < config.seal_hold_min_fd_to_circ_pct:
                return "t1", config.max_hold_days
    else:
        if int(b_strength.get("b_open_times") or 0) > config.seal_hold_max_open_times:
            return "t1", config.max_hold_days
    return "seal_hold", config.seal_hold_max_days


def _find_auction_gap_entry(
    store: DuckDBStore,
    candidate: pd.Series,
    minutes: pd.DataFrame,
    config: AuctionGapMinuteReplayConfig,
) -> tuple[PaperPosition, dict[str, object], SignalProvenance] | None:
    signal_date = _as_date(candidate["signal_date"])
    day_minutes = minutes[
        pd.to_datetime(minutes["trade_time"]).dt.date == signal_date
    ].sort_values("trade_time").reset_index(drop=True)
    if day_minutes.empty:
        return None

    auction_price = float(candidate["entry_price"])
    limit_up_price = float(candidate["limit_up_price"])
    cum_low = float("inf")
    cum_high = float("-inf")
    cum_vol = 0.0
    cum_amount = 0.0
    amount_history: list[tuple[time, float]] = []

    for idx, row in day_minutes.iterrows():
        quote_time = _as_datetime(row["trade_time"])
        minute_vol = float(row["vol"]) if pd.notna(row["vol"]) else 0.0
        minute_amount = float(row["amount"]) if pd.notna(row["amount"]) else 0.0
        cum_low = min(cum_low, float(row["low"]))
        cum_high = max(cum_high, float(row["high"]))
        cum_vol += minute_vol
        cum_amount += minute_amount
        vwap = cum_amount / cum_vol if cum_vol > 0 else None

        if quote_time.time() < config.entry_start_time:
            amount_history.append((quote_time.time(), minute_amount))
            continue

        if limit_up_price <= auction_price:
            amount_history.append((quote_time.time(), minute_amount))
            continue
        support_ok = cum_low >= auction_price * (1 - config.entry_pullback_tolerance_pct)
        price_floor = auction_price
        if vwap is not None:
            price_floor = max(price_floor, vwap * (1 + config.entry_vwap_buffer_pct))
        quote = _minute_quote(row)
        vwap_ok = quote.price >= price_floor
        limit_progress = (cum_high - auction_price) / (limit_up_price - auction_price)
        progress_ok = limit_progress >= config.min_limit_progress_pct
        if not (support_ok and vwap_ok and progress_ok):
            amount_history.append((quote_time.time(), minute_amount))
            continue

        execution_index = idx + 1
        if execution_index >= len(day_minutes):
            return None
        execution_row = day_minutes.iloc[execution_index]
        execution_quote = _execution_quote(execution_row)
        execution_time = _as_datetime(execution_row["trade_time"])
        item = _AuctionWatchItem(
            ts_code=str(candidate["ts_code"]),
            pool="auction_gap",
            name=str(candidate.get("name") or ""),
            entry_date=signal_date,
            reference_date=signal_date,
            limit_up_date=signal_date,
            t_close=float(candidate["pre_close"]),
            t_high=float(candidate["pre_high"]),
            limit_up_price_next=limit_up_price,
            stop_weak=0.0,
        )
        intraday_features = build_intraday_relative_volume_features(
            store,
            str(candidate["ts_code"]),
            quote_time,
            current_minute_amount=minute_amount,
            current_cum_amount=cum_amount,
            current_day_amounts=amount_history,
            freq=config.freq,
        )
        # 键名对齐 signal_provenance.AUCTION_GAP_V1_FACTORS（与全景页/未来 live 同一份 spec）
        factor_values: dict[str, object] = {
            "auction_vol_ratio_5d": float(candidate["auction_vol_ratio_5d"]),
            "gap_pct_close": float(candidate["gap_pct_close"]),
            "vwap_position": quote.price / price_floor if price_floor > 0 else None,
            "limit_progress": limit_progress,
            "support_ok": int(support_ok),
            "rel_amount_same_minute_20d": intraday_features.get(
                "signal_rel_amount_same_minute_20d"
            ),
            "amount_accel_5m": intraday_features.get("signal_amount_accel_5m"),
        }
        factor_score: float | None = None
        if config.factor_score_threshold is not None:
            factor_score = score_feature_terms(
                factor_values, AUCTION_GAP_B_V1_SCORE_TERMS
            )
            if factor_score < config.factor_score_threshold:
                amount_history.append((quote_time.time(), minute_amount))
                continue
        signal_features = {
            "auction_price": auction_price,
            "auction_vol_ratio_5d": float(candidate["auction_vol_ratio_5d"]),
            "auction_amount": float(candidate["auction_amount"]),
            "auction_turnover_rate": (
                float(candidate["auction_turnover_rate"])
                if pd.notna(candidate["auction_turnover_rate"])
                else None
            ),
            "auction_gap_pct_close": float(candidate["gap_pct_close"]),
            "auction_entry_to_limit_up_pct": float(candidate["entry_to_limit_up_pct"]),
            "entry_signal_time": quote_time,
            "entry_signal_price": quote.price,
            "entry_signal_vwap": vwap,
            "entry_signal_cum_low": cum_low,
            "entry_signal_cum_high": cum_high,
            "entry_signal_limit_progress": round(limit_progress, 4),
        }
        signal_features.update(intraday_features)
        if config.factor_score_threshold is not None:
            signal_features["auction_factor_score"] = factor_score
            signal_features["auction_factor_score_threshold"] = (
                config.factor_score_threshold
            )
        provenance = SignalProvenance(
            strategy_name=AUCTION_GAP_MINUTE_STRATEGY,
            signal_factors=build_signal_factors(
                factor_values,
                threshold_overrides={
                    "auction_vol_ratio_5d": config.min_auction_vol_ratio_5d,
                    "limit_progress": config.min_limit_progress_pct,
                },
            ),
            raw_payload=signal_features,
            as_of_time=quote_time,
            lookback_days=20,
        )
        risk_plan = PaperRiskPlan(payload=signal_features)
        position = open_position_from_signal(
            item,
            execution_quote,
            {
                "level": f"auction_gap_{config.entry_mode}",
                "trigger_type": f"auction_gap_{config.entry_mode}",
                "level_price": limit_up_price,
                "signal_price": quote.price,
            },
            execution_time,
            config.paper,
            earliest_exit_date=_as_date(candidate["next_trade_date"]),
            risk_plan=risk_plan,
        )
        return position, signal_features, provenance
    return None


def _query_next_auction(
    store: DuckDBStore,
    ts_code: str,
    trading_date: date,
) -> pd.Series | None:
    decision_at = datetime.combine(
        trading_date,
        time(9, 30),
        tzinfo=EXCHANGE_TIMEZONE,
    )
    sources = visible_sources_at(
        "auction_bar",
        event_date=trading_date,
        as_of_time=decision_at,
    )
    if not sources:
        return None
    df = query_visible_rows(
        store,
        "auction_bar",
        decision_at,
        scope=VisibilityQueryScope(
            ts_codes=(ts_code,),
            start_date=trading_date,
            end_date=trading_date,
            sources=sources,
            columns=(
                "ts_code",
                "trade_date",
                "auction_type",
                "price",
                "vol",
                "amount",
                "turnover_rate",
                "volume_ratio",
                "source",
            ),
        ),
    )
    df = df.loc[df["auction_type"] == "open_realtime"]
    if df.empty:
        return None
    return df.iloc[0]


def _next_auction_is_weak(
    position: PaperPosition,
    *,
    next_auction_price: float,
    b_day_close: float,
    b_strength: dict[str, object],
    config: AuctionGapMinuteReplayConfig,
) -> bool:
    if next_auction_price < position.entry_price:
        return True
    if b_day_close > 0:
        gap_pct = next_auction_price / b_day_close - 1
        close_minutes = int(b_strength.get("b_limit_up_close_minutes") or 0)
        close_at_limit = bool(b_strength.get("b_close_at_limit_up"))
        threshold = (
            config.strong_seal_weak_gap_pct
            if close_at_limit and close_minutes >= config.strong_seal_min_close_minutes
            else config.next_auction_weak_gap_pct
        )
        return gap_pct <= threshold
    return False


def _run_daily_tail_exit(
    store: DuckDBStore,
    position: PaperPosition,
    window_dates: list[date],
    *,
    last_scan_date: date,
    config: AuctionGapMinuteReplayConfig,
) -> PaperPosition | None:
    """分钟数据没铺满持有窗口时，剩余交易日降级用日线近似退出。

    多日持有（seal_hold）常见：候选分钟回补只覆盖 B 日到 T+1 窗口。逐日按
    「开盘缺口 → 日内 low 触及止损/移动止盈 → 收盘更新移动止盈」近似复用
    check_position_exit 语义，最后一个有日线的窗口日收盘平仓。全部缺日线时
    返回 None，由调用方退回按最后一根分钟收盘平仓。
    """
    tail_dates = [trading_date for trading_date in window_dates if trading_date > last_scan_date]
    previous_date = last_scan_date
    last_seen: tuple[date, float] | None = None
    for trading_date in tail_dates:
        daily = _query_daily_bar(store, position.ts_code, trading_date)
        if daily is None or pd.isna(daily["close"]):
            continue
        previous_daily = _query_daily_bar(store, position.ts_code, previous_date)
        if (
            previous_daily is not None
            and pd.notna(previous_daily["close"])
            and pd.notna(daily["pre_close"])
        ):
            position = adjust_open_position_price_basis(
                position,
                float(previous_daily["close"]),
                float(daily["pre_close"]),
            )
        holding_days = _holding_days(window_dates, position.trade_date, trading_date)
        open_price = float(daily["open"])
        open_quote = _AuctionMinuteQuote(
            ts_code=position.ts_code,
            price=open_price,
            low=open_price,
            high=open_price,
        )
        open_event = check_position_exit(
            position,
            open_quote,
            datetime.combine(trading_date, time(9, 30)),
            config.paper,
        )
        if open_event is not None:
            return _close_position(
                position,
                open_event.exit_time,
                open_event.exit_price,
                open_event.exit_reason,
                holding_days,
                {"exit_daily_fallback": True},
            )
        day_quote = _AuctionMinuteQuote(
            ts_code=position.ts_code,
            price=float(daily["close"]),
            low=float(daily["low"]),
            high=float(daily["high"]) if pd.notna(daily["high"]) else None,
        )
        day_event = check_position_exit(
            position,
            day_quote,
            datetime.combine(trading_date, time(15, 0)),
            config.paper,
        )
        if day_event is not None:
            return _close_position(
                position,
                day_event.exit_time,
                day_event.exit_price,
                day_event.exit_reason,
                holding_days,
                {"exit_daily_fallback": True},
            )
        position = mark_position_to_quote(position, day_quote)
        last_seen = (trading_date, float(daily["close"]))
        previous_date = trading_date

    if last_seen is None:
        return None
    exit_date, exit_close = last_seen
    holding_days = _holding_days(window_dates, position.trade_date, exit_date)
    return _close_position(
        position,
        datetime.combine(exit_date, time(15, 0)),
        exit_close,
        f"time_{holding_days}d",
        holding_days,
        {"exit_daily_fallback": True},
    )


def _run_auction_gap_exit_scan(
    store: DuckDBStore,
    position: PaperPosition,
    minutes: pd.DataFrame,
    window_dates: list[date],
    candidate: pd.Series,
    b_strength: dict[str, object],
    config: AuctionGapMinuteReplayConfig,
) -> PaperPosition | None:
    if minutes.empty:
        return None

    next_date = _as_date(candidate["next_trade_date"])
    b_day_close = float(candidate["day_close"]) if pd.notna(candidate["day_close"]) else 0.0
    next_auction = _query_next_auction(store, position.ts_code, next_date)
    # price <= 0 是坏行情行（实测 auction_bar 偶发 price=0 的采集残行），
    # 按无竞价数据处理，落到分钟扫描/日线降级，避免 -100% 假成交
    if (
        next_auction is not None
        and pd.notna(next_auction["price"])
        and float(next_auction["price"]) > 0
    ):
        next_auction_price = float(next_auction["price"])
        next_auction_gap_pct = (
            (next_auction_price / b_day_close - 1) * 100
            if b_day_close > 0
            else None
        )
        if _next_auction_is_weak(
            position,
            next_auction_price=next_auction_price,
            b_day_close=b_day_close,
            b_strength=b_strength,
            config=config,
        ):
            return _close_position(
                position,
                datetime.combine(next_date, time(9, 30)),
                next_auction_price,
                "next_auction_weak",
                _holding_days(window_dates, position.trade_date, next_date),
                {
                    "exit_next_auction_price": next_auction_price,
                    "exit_next_auction_gap_pct": next_auction_gap_pct,
                    "exit_next_auction_amount": (
                        float(next_auction["amount"])
                        if pd.notna(next_auction["amount"])
                        else None
                    ),
                    "exit_next_auction_volume_ratio": (
                        float(next_auction["volume_ratio"])
                        if pd.notna(next_auction["volume_ratio"])
                        else None
                    ),
                },
            )

    scan_minutes = minutes.sort_values("trade_time")
    active_date = position.entry_time.date()
    cum_vol_by_date: dict[date, float] = {}
    cum_amount_by_date: dict[date, float] = {}
    amount_history_by_date: dict[date, list[tuple[time, float]]] = {}

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

        minute_vol = float(row["vol"]) if pd.notna(row["vol"]) else 0.0
        minute_amount = float(row["amount"]) if pd.notna(row["amount"]) else 0.0
        cum_vol_by_date[quote_date] = cum_vol_by_date.get(quote_date, 0.0) + minute_vol
        cum_amount_by_date[quote_date] = (
            cum_amount_by_date.get(quote_date, 0.0) + minute_amount
        )
        day_vwap = (
            cum_amount_by_date[quote_date] / cum_vol_by_date[quote_date]
            if cum_vol_by_date[quote_date] > 0
            else None
        )
        prior_amounts = amount_history_by_date.setdefault(quote_date, [])
        exit_intraday_features: dict[str, object] = {}
        if quote_date >= position.earliest_exit_date:
            raw_exit_features = build_intraday_relative_volume_features(
                store,
                position.ts_code,
                quote_time,
                current_minute_amount=minute_amount,
                current_cum_amount=cum_amount_by_date[quote_date],
                current_day_amounts=prior_amounts,
                freq=config.freq,
            )
            exit_intraday_features = {
                f"exit_{key.removeprefix('signal_')}": value
                for key, value in raw_exit_features.items()
            }

        quote = _minute_quote(row)
        if (
            quote_date >= position.earliest_exit_date
            and quote_time.time() <= config.next_morning_exit_until
            and day_vwap is not None
            and quote.price < position.entry_price
            and quote.price < day_vwap * (1 - config.next_morning_vwap_break_buffer_pct)
        ):
            return _close_position(
                position,
                quote_time,
                quote.price,
                "next_morning_vwap_break",
                _holding_days(window_dates, position.trade_date, quote_date),
                exit_intraday_features,
            )

        exit_event = check_position_exit(position, quote, quote_time, config.paper)
        if exit_event is None:
            position = mark_position_to_quote(position, quote)
            prior_amounts.append((quote_time.time(), minute_amount))
            continue

        return _close_position(
            position,
            exit_event.exit_time,
            exit_event.exit_price,
            exit_event.exit_reason,
            _holding_days(window_dates, position.trade_date, exit_event.exit_time.date()),
            exit_intraday_features,
        )

    last = scan_minutes.iloc[-1]
    last_time = _as_datetime(last["trade_time"])
    if last_time.date() < window_dates[-1]:
        tail_exit = _run_daily_tail_exit(
            store,
            position,
            window_dates,
            last_scan_date=last_time.date(),
            config=config,
        )
        if tail_exit is not None:
            return tail_exit
    return _close_position(
        position,
        last_time,
        float(last["close"]),
        f"time_{_holding_days(window_dates, position.trade_date, last_time.date())}d",
        _holding_days(window_dates, position.trade_date, last_time.date()),
    )


def _position_to_auction_gap_row(
    position: PaperPosition,
    candidate: pd.Series,
    b_strength: dict[str, object],
    hold_policy: HoldPolicy,
) -> dict[str, object]:
    payload = position.risk_payload or {}
    return {
        "ts_code": position.ts_code,
        "name": position.name,
        "strategy": "auction_gap_minute",
        "hold_policy": hold_policy,
        "signal_date": candidate["signal_date"],
        "buy_date": position.trade_date,
        "auction_price": payload.get("auction_price"),
        "auction_vol_ratio_5d": payload.get("auction_vol_ratio_5d"),
        "auction_amount": payload.get("auction_amount"),
        "auction_turnover_rate": payload.get("auction_turnover_rate"),
        "auction_gap_pct_close": payload.get("auction_gap_pct_close"),
        "auction_entry_to_limit_up_pct": payload.get("auction_entry_to_limit_up_pct"),
        "entry_signal_time": payload.get("entry_signal_time"),
        "entry_signal_price": payload.get("entry_signal_price"),
        "entry_signal_vwap": payload.get("entry_signal_vwap"),
        "entry_signal_limit_progress": payload.get("entry_signal_limit_progress"),
        "auction_factor_score": payload.get("auction_factor_score"),
        "auction_factor_score_threshold": payload.get("auction_factor_score_threshold"),
        "entry_signal_minute_amount": payload.get("signal_minute_amount"),
        "entry_signal_cum_amount_asof": payload.get("signal_cum_amount_asof"),
        "entry_hist_same_minute_amount_median_20d": payload.get(
            "hist_same_minute_amount_median_20d"
        ),
        "entry_hist_cum_amount_asof_median_20d": payload.get(
            "hist_cum_amount_asof_median_20d"
        ),
        "entry_signal_rel_amount_same_minute_20d": payload.get(
            "signal_rel_amount_same_minute_20d"
        ),
        "entry_signal_rel_cum_amount_asof_20d": payload.get(
            "signal_rel_cum_amount_asof_20d"
        ),
        "entry_signal_opening_segment": payload.get("signal_opening_segment"),
        "entry_signal_opening_segment_amount": payload.get(
            "signal_opening_segment_amount"
        ),
        "entry_signal_amount_accel_5m": payload.get("signal_amount_accel_5m"),
        "entry_signal_amount_accel_10m": payload.get("signal_amount_accel_10m"),
        "entry_time": position.entry_time,
        "entry_price_raw": position.entry_price_raw,
        "entry_price": position.entry_price,
        "entry_signal": position.entry_signal,
        "b_hit_limit_up_today": candidate["hit_limit_up_today"],
        "b_first_limit_up_time": b_strength.get("b_first_limit_up_time"),
        "b_limit_up_touch_minutes": b_strength.get("b_limit_up_touch_minutes"),
        "b_limit_up_close_minutes": b_strength.get("b_limit_up_close_minutes"),
        "b_open_times": b_strength.get("b_open_times"),
        "b_close_at_limit_up": b_strength.get("b_close_at_limit_up"),
        "next_trade_date": candidate["next_trade_date"],
        "stop_loss_price": position.stop_loss_price,
        "stop_loss_basis": position.stop_loss_basis,
        "take_profit_price": position.take_profit_price,
        "trailing_stop_price": position.trailing_stop_price,
        "exit_next_auction_price": payload.get("exit_next_auction_price"),
        "exit_next_auction_gap_pct": payload.get("exit_next_auction_gap_pct"),
        "exit_next_auction_amount": payload.get("exit_next_auction_amount"),
        "exit_next_auction_volume_ratio": payload.get(
            "exit_next_auction_volume_ratio"
        ),
        "exit_minute_amount": payload.get("exit_minute_amount"),
        "exit_cum_amount_asof": payload.get("exit_cum_amount_asof"),
        "exit_rel_amount_same_minute_20d": payload.get(
            "exit_rel_amount_same_minute_20d"
        ),
        "exit_rel_cum_amount_asof_20d": payload.get("exit_rel_cum_amount_asof_20d"),
        "exit_opening_segment": payload.get("exit_opening_segment"),
        "exit_opening_segment_amount": payload.get("exit_opening_segment_amount"),
        "exit_amount_accel_5m": payload.get("exit_amount_accel_5m"),
        "exit_amount_accel_10m": payload.get("exit_amount_accel_10m"),
        "exit_time": position.exit_time,
        "exit_price": position.exit_price,
        "exit_reason": position.exit_reason,
        "exit_daily_fallback": bool(payload.get("exit_daily_fallback") or False),
        "holding_trading_days": position.holding_trading_days,
        "ret_pct": position.pnl_pct,
    }


def run_auction_gap_replay(
    store: DuckDBStore,
    config: AuctionGapConfig,
) -> pd.DataFrame:
    """按集合竞价跳空条件生成候选，并标注当日/次日结果。

    选股特征只使用信号日集合竞价和信号日前历史日线；结果列使用信号日收盘后
    与次一交易日数据，用于回测评估。
    """
    start_date = _as_date(config.start_date)
    end_date = _as_date(config.end_date)
    decision_at_end = datetime.combine(
        end_date,
        config.decision_time,
        tzinfo=EXCHANGE_TIMEZONE,
    )
    visible_sources = visible_sources_at(
        "auction_bar",
        event_date=end_date,
        as_of_time=decision_at_end,
    )
    if not visible_sources:
        return pd.DataFrame(columns=_AUCTION_GAP_MINIMUM_COLUMNS)
    auction = query_visible_rows(
        store,
        "auction_bar",
        decision_at_end,
        scope=VisibilityQueryScope(
            start_date=start_date,
            end_date=end_date,
            sources=visible_sources,
            columns=(
                "ts_code",
                "trade_date",
                "auction_type",
                "price",
                "vol",
                "amount",
                "turnover_rate",
                "volume_ratio",
                "source",
            ),
        ),
    )
    auction = auction.loc[
        (auction["auction_type"] == "open_realtime")
        & auction["price"].notna()
        & auction["vol"].gt(0)
    ].copy()
    if auction.empty:
        return pd.DataFrame(columns=_AUCTION_GAP_MINIMUM_COLUMNS)
    status = store._conn.execute(
        """
        SELECT status.ts_code,
               status.trade_date,
               status.name AS status_name,
               status.is_st AS status_is_st,
               status.available_at AS status_available_at,
               status.conflict_reason AS status_conflict_reason
        FROM stock_status_daily AS status
        WHERE status.trade_date >= ? AND status.trade_date <= ?
        """,
        [start_date, end_date],
    ).fetchdf()
    auction = auction.merge(
        status,
        on=["ts_code", "trade_date"],
        how="left",
    )
    signal_dates = pd.to_datetime(auction["trade_date"])
    decision_at = (
        signal_dates.dt.tz_localize("Asia/Shanghai")
        + pd.to_timedelta(
            config.decision_time.hour * 60 + config.decision_time.minute,
            unit="minutes",
        )
    )
    available_at = pd.to_datetime(auction["status_available_at"], utc=True)
    status_known = (
        auction["status_conflict_reason"].isna()
        & auction["status_is_st"].notna()
        & available_at.notna()
        & available_at.le(decision_at)
    ).fillna(False)
    auction = auction.loc[status_known].copy()
    if auction.empty:
        return pd.DataFrame(columns=_AUCTION_GAP_MINIMUM_COLUMNS)
    auction["name"] = (
        auction["status_name"]
        .astype("string")
        .str.strip()
        .fillna(auction["ts_code"].astype("string"))
    )
    auction["is_st"] = auction["status_is_st"].astype(bool)
    auction = auction.drop(columns=[
        "status_name",
        "status_is_st",
        "status_available_at",
        "status_conflict_reason",
    ])

    daily = store._conn.execute(
        """
        SELECT ts_code, trade_date, open, high, low, close, vol
        FROM daily_bar
        WHERE trade_date <= (
            SELECT MAX(trade_date) FROM daily_bar
        )
        """
    ).fetchdf()
    state = store._conn.execute(
        """
        SELECT ts_code, trade_date, is_st, is_limit_up, is_yiziban, is_limit_down
        FROM daily_state
        """
    ).fetchdf()
    for frame in (auction, daily, state):
        frame["trade_date"] = pd.to_datetime(frame["trade_date"])

    daily = daily.sort_values(["ts_code", "trade_date"]).reset_index(drop=True)
    grouped = daily.groupby("ts_code", sort=False)
    daily["avg_vol_5d_asof"] = (
        grouped["vol"]
        .rolling(5, min_periods=5)
        .mean()
        .reset_index(level=0, drop=True)
        * config.daily_vol_unit_factor
    )
    daily["prev5_count_asof"] = (
        grouped["vol"]
        .rolling(5, min_periods=5)
        .count()
        .reset_index(level=0, drop=True)
    )
    prior_daily_features = daily.rename(
        columns={
            "trade_date": "prev_trade_date",
            "close": "pre_close",
            "high": "pre_high",
            "avg_vol_5d_asof": "avg_vol_5d",
            "prev5_count_asof": "prev5_count",
        }
    )[
        [
            "ts_code", "prev_trade_date", "pre_close", "pre_high",
            "avg_vol_5d", "prev5_count",
        ]
    ]
    day_daily_features = daily.rename(
        columns={
            "open": "day_open",
            "high": "day_high",
            "low": "day_low",
            "close": "day_close",
        }
    )[["ts_code", "trade_date", "day_open", "day_high", "day_low", "day_close"]]

    calendar = (
        daily[["trade_date"]]
        .drop_duplicates()
        .sort_values("trade_date")
        .reset_index(drop=True)
    )
    calendar["next_trade_date"] = calendar["trade_date"].shift(-1)

    requested_codes = sorted(auction["ts_code"].astype(str).unique().tolist())
    listing = store._conn.execute(
        """
        WITH requested AS (
            SELECT UNNEST(?) AS ts_code
        ),
        first_daily AS (
            SELECT ts_code, MIN(trade_date) AS first_trade_date
            FROM daily_bar
            WHERE ts_code IN (SELECT ts_code FROM requested)
            GROUP BY ts_code
        )
        SELECT requested.ts_code,
               COALESCE(basic.list_date, first_daily.first_trade_date) AS list_date
        FROM requested
        LEFT JOIN stock_basic AS basic USING (ts_code)
        LEFT JOIN first_daily USING (ts_code)
        """,
        [requested_codes],
    ).fetchdf()
    listing_calendar = store._conn.execute(
        """
        SELECT cal_date
        FROM trade_calendar
        WHERE exchange = 'SSE' AND is_open = TRUE
        ORDER BY cal_date
        """
    ).fetchdf()
    open_calendar = [
        pd.Timestamp(value).date()
        for value in listing_calendar.get("cal_date", pd.Series(dtype="object"))
    ]
    listing_facts: dict[str, ListingPriceLimitFact] = {}
    for listing_row in listing.itertuples(index=False):
        if pd.isna(listing_row.list_date):
            continue
        list_date = pd.Timestamp(listing_row.list_date).date()
        listing_sessions = [day for day in open_calendar if day >= list_date]
        fifth_listing_trade_date = (
            listing_sessions[4] if len(listing_sessions) >= 5 else None
        )
        listing_facts[str(listing_row.ts_code)] = ListingPriceLimitFact(
            list_date=list_date,
            fifth_listing_trade_date=fifth_listing_trade_date,
        )

    current_state = state.rename(
        columns={
            "is_st": "state_is_st",
            "is_limit_up": "hit_limit_up_today",
            "is_yiziban": "hit_yiziban_today",
        }
    )
    next_daily = daily.rename(
        columns={
            "trade_date": "next_trade_date",
            "open": "next_open",
            "high": "next_high",
            "low": "next_low",
            "close": "next_close",
        }
    )[["ts_code", "next_trade_date", "next_open", "next_high", "next_low", "next_close"]]
    next_state = state.rename(
        columns={
            "trade_date": "next_trade_date",
            "is_limit_down": "next_limit_down",
            "is_yiziban": "next_yiziban",
        }
    )[["ts_code", "next_trade_date", "next_limit_down", "next_yiziban"]]

    auction_signals = auction.rename(
        columns={
            "trade_date": "signal_date",
            "price": "entry_price",
            "vol": "auction_vol",
            "amount": "auction_amount",
            "turnover_rate": "auction_turnover_rate",
            "volume_ratio": "auction_volume_ratio",
        }
    )
    auction_signals = _merge_prior_daily_features(
        auction_signals,
        prior_daily_features,
    )

    out = (
        auction_signals
        .merge(
            day_daily_features.rename(columns={"trade_date": "signal_date"}),
            on=["ts_code", "signal_date"],
            how="left",
        )
        .merge(
            current_state.rename(columns={"trade_date": "signal_date"}),
            on=["ts_code", "signal_date"],
            how="left",
        )
        .merge(
            calendar.rename(columns={"trade_date": "signal_date"}),
            on="signal_date",
            how="left",
        )
        .merge(next_daily, on=["ts_code", "next_trade_date"], how="left")
        .merge(next_state, on=["ts_code", "next_trade_date"], how="left")
    )
    out["board_type"] = out["ts_code"].apply(_classify_board)
    state_matches_status = (
        out["state_is_st"].notna()
        & out["state_is_st"].astype("boolean").eq(out["is_st"].astype("boolean"))
    ).fillna(False)
    limit_pcts: list[float | None] = []
    for row in out.itertuples(index=False):
        signal_date = pd.Timestamp(row.signal_date).date()
        board_type = str(row.board_type)
        fact = listing_facts.get(str(row.ts_code))
        eligible = _price_limit_eligibility(
            [signal_date],
            board_type=board_type,
            fact=fact,
        ).iloc[0]
        if pd.isna(eligible) or not bool(eligible):
            limit_pcts.append(None)
            continue
        limit_pcts.append(
            _historical_limit_pct(bool(row.is_st), board_type, signal_date)
        )
    out["limit_pct"] = pd.Series(limit_pcts, index=out.index, dtype="Float64")
    out["limit_up_price"] = _round_half_up(
        out["pre_close"] * (1 + out["limit_pct"])
    ).where(out["limit_pct"].notna())
    if "hit_limit_up_today" in out.columns:
        calc_hit_limit = pd.Series(pd.NA, index=out.index, dtype="boolean")
        has_day_close = out["day_close"].notna() & out["limit_up_price"].notna()
        calc_hit_limit.loc[has_day_close] = (
            out.loc[has_day_close, "day_close"]
            >= out.loc[has_day_close, "limit_up_price"] - config.price_tol
        )
        out["hit_limit_up_today"] = (
            out["hit_limit_up_today"]
            .astype("boolean")
            .where(state_matches_status)
            .combine_first(calc_hit_limit)
        )
    if "hit_yiziban_today" in out.columns:
        out["hit_yiziban_today"] = (
            out["hit_yiziban_today"].astype("boolean").where(state_matches_status)
        )
    out = out[out["prev5_count"] == 5].copy()
    out["auction_vol_ratio_5d"] = out["auction_vol"] / out["avg_vol_5d"]
    out["gap_pct_close"] = (out["entry_price"] / out["pre_close"] - 1) * 100
    out["gap_pct_high"] = (out["entry_price"] / out["pre_high"] - 1) * 100

    if config.gap_mode == "close":
        mask = out["entry_price"] > out["pre_close"]
    elif config.gap_mode == "strict_high":
        mask = out["entry_price"] > out["pre_high"]
    else:
        raise ValueError(f"unsupported gap_mode: {config.gap_mode}")

    mask &= out["auction_vol_ratio_5d"].between(
        config.min_auction_vol_ratio_5d,
        config.max_auction_vol_ratio_5d,
        inclusive="both",
    )
    mask &= out["entry_price"] < out["limit_up_price"] - config.price_tol
    if config.st_filter == "case_insensitive":
        mask &= ~out["is_st"].astype(bool)
    elif config.st_filter == "literal_lower":
        mask &= ~out["name"].str.contains("st", regex=False, na=False)
    elif config.st_filter != "none":
        raise ValueError(f"unsupported st_filter: {config.st_filter}")
    if config.require_next_day:
        mask &= out["next_open"].notna()

    out = out[mask].copy()
    out["entry_to_limit_up_pct"] = (
        out["limit_up_price"] / out["entry_price"] - 1
    ) * 100
    out["intraday_high_ret_pct"] = (out["day_high"] / out["entry_price"] - 1) * 100
    out["intraday_low_ret_pct"] = (out["day_low"] / out["entry_price"] - 1) * 100
    out["day_close_ret_pct"] = (out["day_close"] / out["entry_price"] - 1) * 100
    out["next_open_ret_pct"] = (out["next_open"] / out["entry_price"] - 1) * 100
    out["next_close_ret_pct"] = (out["next_close"] / out["entry_price"] - 1) * 100
    out["next_high_ret_pct"] = (out["next_high"] / out["entry_price"] - 1) * 100
    out["next_low_ret_pct"] = (out["next_low"] / out["entry_price"] - 1) * 100

    return out.sort_values(["signal_date", "ts_code"]).reset_index(drop=True)


def summarize_auction_gap_replay(trades: pd.DataFrame) -> AuctionGapSummary:
    if trades.empty:
        return AuctionGapSummary(trades_count=0)

    hit_limit = trades["hit_limit_up_today"].fillna(False).astype(bool)
    next_ret = trades["next_open_ret_pct"].dropna()
    intraday_high = trades["intraday_high_ret_pct"].dropna()
    intraday_low = trades["intraday_low_ret_pct"].dropna()

    return AuctionGapSummary(
        trades_count=len(trades),
        hit_limit_up_rate_pct=float(hit_limit.mean() * 100),
        next_open_win_rate_pct=float((next_ret > 0).mean() * 100)
        if len(next_ret)
        else None,
        next_open_mean_ret_pct=float(next_ret.mean()) if len(next_ret) else None,
        next_open_median_ret_pct=float(next_ret.median()) if len(next_ret) else None,
        intraday_high_mean_ret_pct=float(intraday_high.mean())
        if len(intraday_high)
        else None,
        intraday_max_drawdown_mean_pct=float(intraday_low.mean())
        if len(intraday_low)
        else None,
    )


def run_auction_gap_minute_replay(
    store: DuckDBStore,
    config: AuctionGapMinuteReplayConfig,
    *,
    persist_positions: bool = False,
    run_id: str | None = None,
    candidates: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """集合竞价只做候选池，B/S 均由可见分钟因子确认。

    B 日：集合竞价候选进入观察池，开盘后等待分钟线确认强承接与向涨停推进，
    并用下一分钟开盘价成交。

    S 日：先看次日集合竞价是否明显转弱；若不弱，再进入分钟线止损、移动止盈
    与早盘 VWAP 破位扫描。持有时间上限默认 max_hold_days；开启 seal_hold 时
    B 日收盘封住且封板质量达标的仓位放宽到 seal_hold_max_days。

    ``candidates`` 可传入 ``run_auction_gap_replay`` 的既有结果避免重复生成
    （同一候选参数跑多组持有期网格时用）。

    ``persist_positions=True`` 时把闭环后的模拟仓带溯源写进 paper_position
    （run_mode='replay'，同一 ``run_id`` 可整批 DELETE 清理重跑）。落库路径
    必须在非盘中时段跑——盘中 monitor 持主库写锁（CLAUDE.md DuckDB 并发约束）。
    """
    if candidates is None:
        candidates = run_auction_gap_replay(store, config.auction_config())
    if candidates.empty:
        return pd.DataFrame()

    calendar = _trading_dates(store)
    if not calendar:
        return pd.DataFrame()

    effective_run_id = run_id
    if persist_positions and effective_run_id is None:
        effective_run_id = f"auction_gap_replay-{datetime.now():%Y%m%d%H%M%S}"

    max_window_days = config.max_hold_days
    if config.seal_hold_enabled:
        max_window_days = max(max_window_days, config.seal_hold_max_days)

    rows: list[dict[str, object]] = []
    for _, candidate in candidates.iterrows():
        signal_date = _as_date(candidate["signal_date"])
        window_dates_full = _window_trading_dates(calendar, signal_date, max_window_days)
        if len(window_dates_full) <= 1:
            continue
        if _as_date(candidate["next_trade_date"]) not in window_dates_full:
            continue
        _, signal_end = _day_bounds(signal_date)
        _, window_end_full = _day_bounds(window_dates_full[-1])
        minutes = store.query_minute_bars(
            str(candidate["ts_code"]),
            datetime.combine(signal_date, time(9, 30)),
            window_end_full,
            freq=config.freq,
        )
        if minutes.empty:
            continue

        entry = _find_auction_gap_entry(store, candidate, minutes, config)
        if entry is None:
            continue
        position, _signal_features, provenance = entry
        b_strength = _b_day_strength(
            minutes,
            trading_date=signal_date,
            limit_up_price=float(candidate["limit_up_price"]),
            price_tol=config.price_tol,
        )
        hold_policy, hold_days = _resolve_hold_policy(store, candidate, b_strength, config)
        window_dates = window_dates_full[: hold_days + 1]
        _, window_end = _day_bounds(window_dates[-1])
        exit_position = _run_auction_gap_exit_scan(
            store,
            position,
            minutes[
                pd.to_datetime(minutes["trade_time"]) <= window_end
            ],
            window_dates,
            candidate,
            b_strength,
            config,
        )
        if exit_position is None:
            continue
        if persist_positions:
            persist_position_with_provenance(
                store,
                exit_position,
                provenance,
                run_mode="replay",
                run_id=effective_run_id,
                param_payload=config.paper.model_dump(),
            )
        rows.append(
            _position_to_auction_gap_row(exit_position, candidate, b_strength, hold_policy)
        )

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).sort_values(["signal_date", "ts_code"]).reset_index(drop=True)


def summarize_auction_gap_minute_replay(
    trades: pd.DataFrame,
    *,
    candidates_count: int,
) -> AuctionGapMinuteSummary:
    if trades.empty:
        return AuctionGapMinuteSummary(
            candidates_count=candidates_count,
            trades_count=0,
            trigger_rate_pct=0.0 if candidates_count else None,
        )
    ret = trades["ret_pct"].dropna()
    weak_exits = trades["exit_reason"].fillna("").eq("next_auction_weak")
    return AuctionGapMinuteSummary(
        candidates_count=candidates_count,
        trades_count=len(trades),
        trigger_rate_pct=len(trades) / candidates_count * 100 if candidates_count else None,
        mean_ret_pct=float(ret.mean()) if len(ret) else None,
        median_ret_pct=float(ret.median()) if len(ret) else None,
        win_rate_pct=float((ret > 0).mean() * 100) if len(ret) else None,
        next_auction_weak_exit_rate_pct=float(weak_exits.mean() * 100),
    )
