"""集合竞价跳空策略候选生成与日线近似回测。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from typing import Literal

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

from rquant.paper import (
    PaperPosition,
    PaperRiskPlan,
    PaperTradeConfig,
    adjust_open_position_price_basis,
    check_position_exit,
    mark_position_to_quote,
    open_position_from_signal,
)
from rquant.state.derive import _classify_board, _detect_st, _limit_pct, _round_half_up
from rquant.stock_features import build_intraday_relative_volume_features
from rquant.storage.duckdb import DuckDBStore

GapMode = Literal["close", "strict_high"]
StFilterMode = Literal["case_insensitive", "literal_lower", "none"]
ExitMode = Literal["next_open"]
AuctionGapMinuteEntryMode = Literal["vwap_push"]


class AuctionGapConfig(BaseModel):
    """集合竞价跳空策略配置。"""

    model_config = ConfigDict(frozen=True)

    start_date: str
    end_date: str
    gap_mode: GapMode = "close"
    min_auction_vol_ratio_5d: float = 0.15
    max_auction_vol_ratio_5d: float = 5.0
    st_filter: StFilterMode = "case_insensitive"
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
    if minutes.empty or limit_up_price <= 0:
        return {
            "b_first_limit_up_time": None,
            "b_limit_up_touch_minutes": 0,
            "b_limit_up_close_minutes": 0,
            "b_close_at_limit_up": False,
        }
    day_minutes = minutes[
        pd.to_datetime(minutes["trade_time"]).dt.date == trading_date
    ].sort_values("trade_time")
    if day_minutes.empty:
        return {
            "b_first_limit_up_time": None,
            "b_limit_up_touch_minutes": 0,
            "b_limit_up_close_minutes": 0,
            "b_close_at_limit_up": False,
        }

    touch_mask = day_minutes["high"] >= limit_up_price - price_tol
    close_mask = day_minutes["close"] >= limit_up_price - price_tol
    first_time = (
        _as_datetime(day_minutes.loc[touch_mask, "trade_time"].iloc[0])
        if bool(touch_mask.any())
        else None
    )
    return {
        "b_first_limit_up_time": first_time,
        "b_limit_up_touch_minutes": int(touch_mask.sum()),
        "b_limit_up_close_minutes": int(close_mask.sum()),
        "b_close_at_limit_up": bool(close_mask.iloc[-1]) if len(close_mask) else False,
    }


def _find_auction_gap_entry(
    store: DuckDBStore,
    candidate: pd.Series,
    minutes: pd.DataFrame,
    config: AuctionGapMinuteReplayConfig,
) -> tuple[PaperPosition, dict[str, object]] | None:
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
        signal_features.update(
            build_intraday_relative_volume_features(
                store,
                str(candidate["ts_code"]),
                quote_time,
                current_minute_amount=minute_amount,
                current_cum_amount=cum_amount,
                current_day_amounts=amount_history,
                freq=config.freq,
            )
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
        return position, signal_features
    return None


def _query_next_auction(
    store: DuckDBStore,
    ts_code: str,
    trading_date: date,
) -> pd.Series | None:
    df = store._conn.execute(
        """
        SELECT ts_code, trade_date, price, vol, amount, turnover_rate, volume_ratio
        FROM auction_bar
        WHERE ts_code = ?
          AND trade_date = ?
          AND auction_type = 'open_realtime'
        """,
        [ts_code, trading_date],
    ).fetchdf()
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
    if next_auction is not None and pd.notna(next_auction["price"]):
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
) -> dict[str, object]:
    payload = position.risk_payload or {}
    return {
        "ts_code": position.ts_code,
        "name": position.name,
        "strategy": "auction_gap_minute",
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
    auction = store._conn.execute(
        """
        SELECT ts_code, trade_date, price, vol, amount, turnover_rate, volume_ratio,
               source
        FROM auction_bar
        WHERE trade_date >= ?
          AND trade_date <= ?
          AND auction_type = 'open_realtime'
          AND price IS NOT NULL
          AND vol > 0
        """,
        [config.start_date, config.end_date],
    ).fetchdf()
    if auction.empty:
        return pd.DataFrame()
    source_priority = auction["source"].map({
        "tushare": 0,
        "minute_0930_fallback": 1,
    }).fillna(9)
    auction = (
        auction.assign(_source_priority=source_priority)
        .sort_values(["ts_code", "trade_date", "_source_priority"])
        .drop_duplicates(["ts_code", "trade_date"], keep="first")
        .drop(columns=["_source_priority"])
        .reset_index(drop=True)
    )

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
        SELECT ts_code, trade_date, is_st, board_type, limit_pct, limit_up_price,
               is_limit_up, is_yiziban, is_limit_down
        FROM daily_state
        """
    ).fetchdf()
    stock_basic = store._conn.execute(
        "SELECT ts_code, name FROM stock_basic"
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

    current_state = state.rename(
        columns={
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
        .merge(stock_basic, on="ts_code", how="left")
    )
    computed_is_st = out["name"].apply(_detect_st)
    computed_board_type = out["ts_code"].apply(_classify_board)
    computed_limit_pct = pd.Series(
        [
            _limit_pct(bool(is_st), str(board_type))
            for is_st, board_type in zip(
                computed_is_st,
                computed_board_type,
                strict=False,
            )
        ],
        index=out.index,
    )
    computed_limit_up = _round_half_up(out["pre_close"] * (1 + computed_limit_pct))
    out["is_st"] = out["is_st"].combine_first(computed_is_st)
    out["board_type"] = out["board_type"].combine_first(computed_board_type)
    out["limit_pct"] = out["limit_pct"].combine_first(computed_limit_pct)
    out["limit_up_price"] = out["limit_up_price"].combine_first(computed_limit_up)
    if "hit_limit_up_today" in out.columns:
        calc_hit_limit = pd.Series(pd.NA, index=out.index, dtype="boolean")
        has_day_close = out["day_close"].notna() & out["limit_up_price"].notna()
        calc_hit_limit.loc[has_day_close] = (
            out.loc[has_day_close, "day_close"]
            >= out.loc[has_day_close, "limit_up_price"] - config.price_tol
        )
        out["hit_limit_up_today"] = (
            out["hit_limit_up_today"].astype("boolean").combine_first(calc_hit_limit)
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
        mask &= ~out["is_st"].fillna(False).astype(bool)
        mask &= ~out["name"].fillna("").str.lower().str.contains("st", regex=False)
    elif config.st_filter == "literal_lower":
        mask &= ~out["name"].fillna("").str.contains("st", regex=False)
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
) -> pd.DataFrame:
    """集合竞价只做候选池，B/S 均由可见分钟因子确认。

    B 日：集合竞价候选进入观察池，开盘后等待分钟线确认强承接与向涨停推进，
    并用下一分钟开盘价成交。

    S 日：先看次日集合竞价是否明显转弱；若不弱，再进入分钟线止损、移动止盈
    与早盘 VWAP 破位扫描。
    """
    candidates = run_auction_gap_replay(store, config.auction_config())
    if candidates.empty:
        return pd.DataFrame()

    calendar = _trading_dates(store)
    if not calendar:
        return pd.DataFrame()

    rows: list[dict[str, object]] = []
    for _, candidate in candidates.iterrows():
        signal_date = _as_date(candidate["signal_date"])
        window_dates = _window_trading_dates(calendar, signal_date, config.max_hold_days)
        if len(window_dates) <= 1:
            continue
        if _as_date(candidate["next_trade_date"]) not in window_dates:
            continue
        _, signal_end = _day_bounds(signal_date)
        _, window_end = _day_bounds(window_dates[-1])
        minutes = store.query_minute_bars(
            str(candidate["ts_code"]),
            datetime.combine(signal_date, time(9, 30)),
            window_end,
            freq=config.freq,
        )
        if minutes.empty:
            continue

        entry = _find_auction_gap_entry(store, candidate, minutes, config)
        if entry is None:
            continue
        position, _signal_features = entry
        b_strength = _b_day_strength(
            minutes,
            trading_date=signal_date,
            limit_up_price=float(candidate["limit_up_price"]),
            price_tol=config.price_tol,
        )
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
        rows.append(_position_to_auction_gap_row(exit_position, candidate, b_strength))

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
