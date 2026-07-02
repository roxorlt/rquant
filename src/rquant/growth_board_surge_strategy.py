"""科创/创业板盘中放量追击策略回放。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time

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

UNSUPPORTED_ORDER_FLOW_CONDITIONS = (
    "外盘/内盘、今日大单净量缺少可回测的盘中历史订单流数据"
)


class GrowthBoardSurgeConfig(BaseModel):
    """科创/创业板盘中放量追击 replay 参数。"""

    model_config = ConfigDict(frozen=True)

    freq: str = "1min"
    min_signal_time: time = time(9, 33)
    lookback_days: int = Field(default=20, ge=1, le=90)
    min_hist_days: int = Field(default=10, ge=1, le=90)
    min_cum_amount_ratio: float = Field(default=1.4, gt=0)
    min_same_minute_amount_ratio: float = Field(default=2.0, gt=0)
    min_amount_accel_5m: float = Field(default=2.0, gt=0)
    use_same_minute_surge: bool = True
    use_accel_surge: bool = True
    require_vwap_strength: bool = True
    vwap_buffer_pct: float = Field(default=0.0, ge=0, lt=0.05)
    max_hold_days: int = Field(default=1, ge=1, le=10)
    price_tol: float = Field(default=0.01, gt=0, lt=1)
    paper: PaperTradeConfig = Field(
        default_factory=lambda: PaperTradeConfig(
            candidate_id="growth_board_surge_v0",
            stop_loss_pct=0.04,
            take_profit_pct=0.08,
            trailing_stop_pct=0.03,
        )
    )


@dataclass(frozen=True)
class _GrowthBoardCandidate:
    ts_code: str
    name: str
    trade_date: date
    previous_date: date
    board_type: str
    pre_close: float
    limit_up_price: float


@dataclass(frozen=True)
class _WatchItem:
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
class _Quote:
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


def _window_trading_dates(
    calendar: list[date],
    start: date,
    max_hold_days: int,
) -> list[date]:
    return [trading_date for trading_date in calendar if trading_date >= start][
        : max_hold_days + 1
    ]


def _previous_trading_date(calendar: list[date], current: date) -> date | None:
    previous = [trading_date for trading_date in calendar if trading_date < current]
    if not previous:
        return None
    return previous[-1]


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
        SELECT ts_code, trade_date, close, pre_close
        FROM daily_bar
        WHERE ts_code = ?
          AND trade_date = ?
        """,
        [ts_code, trading_date],
    ).fetchdf()
    if df.empty:
        return None
    return df.iloc[0]


def _query_candidates(
    store: DuckDBStore,
    trading_date: date,
    previous_date: date,
) -> list[_GrowthBoardCandidate]:
    raw = store._conn.execute(
        """
        WITH ma_base AS (
            SELECT ts_code,
                   trade_date,
                   AVG(close) OVER (
                       PARTITION BY ts_code
                       ORDER BY trade_date
                       ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                   ) AS ma5_calc,
                   COUNT(close) OVER (
                       PARTITION BY ts_code
                       ORDER BY trade_date
                       ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                   ) AS ma5_count,
                   AVG(close) OVER (
                       PARTITION BY ts_code
                       ORDER BY trade_date
                       ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
                   ) AS ma10_calc,
                   COUNT(close) OVER (
                       PARTITION BY ts_code
                       ORDER BY trade_date
                       ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
                   ) AS ma10_count,
                   AVG(close) OVER (
                       PARTITION BY ts_code
                       ORDER BY trade_date
                       ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                   ) AS ma20_calc,
                   COUNT(close) OVER (
                       PARTITION BY ts_code
                       ORDER BY trade_date
                       ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                   ) AS ma20_count,
                   AVG(close) OVER (
                       PARTITION BY ts_code
                       ORDER BY trade_date
                       ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                   ) AS ma60_calc,
                   COUNT(close) OVER (
                       PARTITION BY ts_code
                       ORDER BY trade_date
                       ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                   ) AS ma60_count
            FROM daily_bar
        ),
        daily_ma AS (
            SELECT ts_code,
                   trade_date,
                   CASE WHEN ma5_count >= 5 THEN ma5_calc END AS ma5_calc,
                   CASE WHEN ma10_count >= 10 THEN ma10_calc END AS ma10_calc,
                   CASE WHEN ma20_count >= 20 THEN ma20_calc END AS ma20_calc,
                   CASE WHEN ma60_count >= 60 THEN ma60_calc END AS ma60_calc
            FROM ma_base
        )
        SELECT db.ts_code,
               db.pre_close,
               sb.name,
               ds.is_st,
               ds.board_type,
               ds.limit_pct,
               ds.limit_up_price,
               COALESCE(di.ma5, dma.ma5_calc) AS ma5,
               COALESCE(di.ma10, dma.ma10_calc) AS ma10,
               COALESCE(di.ma20, dma.ma20_calc) AS ma20,
               COALESCE(di.ma60, dma.ma60_calc) AS ma60
        FROM daily_bar db
        LEFT JOIN stock_basic sb ON db.ts_code = sb.ts_code
        LEFT JOIN daily_state ds
          ON db.ts_code = ds.ts_code AND db.trade_date = ds.trade_date
        LEFT JOIN daily_indicator di
          ON db.ts_code = di.ts_code AND di.trade_date = ?
        LEFT JOIN daily_ma dma
          ON db.ts_code = dma.ts_code AND dma.trade_date = ?
        WHERE db.trade_date = ?
        """,
        [previous_date, previous_date, trading_date],
    ).fetchdf()
    if raw.empty:
        return []

    candidates: list[_GrowthBoardCandidate] = []
    for _, row in raw.iterrows():
        ts_code = str(row["ts_code"])
        name = "" if pd.isna(row["name"]) else str(row["name"])
        is_st = (
            bool(row["is_st"]) if pd.notna(row["is_st"]) else bool(_detect_st(name))
        )
        board_type = (
            str(row["board_type"])
            if pd.notna(row["board_type"]) and str(row["board_type"])
            else _classify_board(ts_code)
        )
        if is_st or board_type not in {"gem", "star"}:
            continue
        ma_values = [row["ma5"], row["ma10"], row["ma20"], row["ma60"]]
        if any(pd.isna(value) for value in ma_values):
            continue
        ma5, ma10, ma20, ma60 = [float(value) for value in ma_values]
        if not (ma5 > ma10 > ma20 > ma60):
            continue
        if pd.isna(row["pre_close"]) or float(row["pre_close"]) <= 0:
            continue
        limit_pct = (
            float(row["limit_pct"])
            if pd.notna(row["limit_pct"])
            else _limit_pct(False, board_type)
        )
        limit_up_price = (
            float(row["limit_up_price"])
            if pd.notna(row["limit_up_price"])
            else float(_round_half_up(float(row["pre_close"]) * (1 + limit_pct)))
        )
        candidates.append(
            _GrowthBoardCandidate(
                ts_code=ts_code,
                name=name,
                trade_date=trading_date,
                previous_date=previous_date,
                board_type=board_type,
                pre_close=float(row["pre_close"]),
                limit_up_price=limit_up_price,
            )
        )
    return candidates


def _quote_from_close(row: pd.Series) -> _Quote:
    return _Quote(
        ts_code=str(row["ts_code"]),
        price=float(row["close"]),
        low=float(row["low"]),
        high=float(row["high"]) if pd.notna(row["high"]) else None,
    )


def _quote_from_open(row: pd.Series) -> _Quote:
    price = float(row["open"])
    return _Quote(ts_code=str(row["ts_code"]), price=price, low=price, high=price)


def _is_intraday_yiziban(
    day_minutes: pd.DataFrame,
    limit_up_price: float,
    price_tol: float,
) -> bool:
    if day_minutes.empty or limit_up_price <= 0:
        return False
    first = day_minutes.iloc[0]
    first_time = _as_datetime(first["trade_time"]).time()
    if first_time != time(9, 30):
        return False
    prices = [first["open"], first["high"], first["low"], first["close"]]
    if any(pd.isna(value) for value in prices):
        return False
    return all(float(value) >= limit_up_price - price_tol for value in prices)


def _extract_relative_volume_features(
    features: dict[str, float | int | None],
    lookback_days: int,
) -> dict[str, float | int | None]:
    suffix = f"{lookback_days}d"
    return {
        "signal_minute_amount": features.get("signal_minute_amount"),
        "signal_cum_amount_asof": features.get("signal_cum_amount_asof"),
        "hist_same_minute_amount_median": features.get(
            f"hist_same_minute_amount_median_{suffix}"
        ),
        "hist_cum_amount_asof_median": features.get(
            f"hist_cum_amount_asof_median_{suffix}"
        ),
        "signal_rel_amount_same_minute": features.get(
            f"signal_rel_amount_same_minute_{suffix}"
        ),
        "signal_rel_cum_amount_asof": features.get(
            f"signal_rel_cum_amount_asof_{suffix}"
        ),
        "hist_intraday_days": features.get(f"hist_intraday_days_{suffix}"),
        "signal_opening_segment": features.get("signal_opening_segment"),
        "signal_opening_segment_amount": features.get("signal_opening_segment_amount"),
        "signal_amount_accel_5m": features.get("signal_amount_accel_5m"),
        "signal_amount_accel_10m": features.get("signal_amount_accel_10m"),
    }


def _passes_surge_filter(
    compact_features: dict[str, float | int | None],
    config: GrowthBoardSurgeConfig,
) -> bool:
    hist_days = compact_features["hist_intraday_days"]
    rel_cum = compact_features["signal_rel_cum_amount_asof"]
    rel_same = compact_features["signal_rel_amount_same_minute"]
    accel_5m = compact_features["signal_amount_accel_5m"]
    if hist_days is None or int(hist_days) < config.min_hist_days:
        return False
    if rel_cum is None or float(rel_cum) < config.min_cum_amount_ratio:
        return False
    if not config.use_same_minute_surge and not config.use_accel_surge:
        return True
    has_same_minute_surge = bool(
        config.use_same_minute_surge
        and rel_same is not None
        and float(rel_same) >= config.min_same_minute_amount_ratio
    )
    has_accel_surge = bool(
        config.use_accel_surge
        and accel_5m is not None
        and float(accel_5m) >= config.min_amount_accel_5m
    )
    return has_same_minute_surge or has_accel_surge


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
    config: GrowthBoardSurgeConfig,
) -> PaperPosition | None:
    if minutes.empty:
        return None
    active_date = position.entry_time.date()
    for _, row in minutes.sort_values("trade_time").iterrows():
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

        quote = _quote_from_close(row)
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

    last = minutes.sort_values("trade_time").iloc[-1]
    last_time = _as_datetime(last["trade_time"])
    if last_time.date() < position.earliest_exit_date:
        return None
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


def _position_to_row(
    position: PaperPosition,
    candidate: _GrowthBoardCandidate,
    day_minutes: pd.DataFrame,
) -> dict[str, object]:
    payload = position.risk_payload or {}
    hit_limit_up_today = bool(
        not day_minutes.empty
        and float(pd.to_numeric(day_minutes["high"], errors="coerce").max())
        >= candidate.limit_up_price - 0.01
    )
    return {
        "ts_code": position.ts_code,
        "name": position.name,
        "board_type": candidate.board_type,
        "signal_date": candidate.trade_date,
        "previous_date": candidate.previous_date,
        "entry_time": position.entry_time,
        "entry_price_raw": position.entry_price_raw,
        "entry_price": position.entry_price,
        "entry_signal": position.entry_signal,
        "limit_up_price": candidate.limit_up_price,
        "entry_to_limit_room_pct": round(
            (candidate.limit_up_price / position.entry_price - 1) * 100,
            4,
        ),
        "hit_limit_up_today": hit_limit_up_today,
        "stop_loss_price": position.stop_loss_price,
        "take_profit_price": position.take_profit_price,
        "trailing_stop_price": position.trailing_stop_price,
        "exit_time": position.exit_time,
        "exit_price": position.exit_price,
        "exit_reason": position.exit_reason,
        "holding_trading_days": position.holding_trading_days,
        "ret_pct": position.pnl_pct,
        "signal_minute_amount": payload.get("signal_minute_amount"),
        "signal_cum_amount_asof": payload.get("signal_cum_amount_asof"),
        "hist_same_minute_amount_median": payload.get(
            "hist_same_minute_amount_median"
        ),
        "hist_cum_amount_asof_median": payload.get("hist_cum_amount_asof_median"),
        "signal_rel_amount_same_minute": payload.get("signal_rel_amount_same_minute"),
        "signal_rel_cum_amount_asof": payload.get("signal_rel_cum_amount_asof"),
        "hist_intraday_days": payload.get("hist_intraday_days"),
        "signal_opening_segment": payload.get("signal_opening_segment"),
        "signal_amount_accel_5m": payload.get("signal_amount_accel_5m"),
        "signal_amount_accel_10m": payload.get("signal_amount_accel_10m"),
        "signal_vwap": payload.get("signal_vwap"),
        "signal_limit_progress_pct": payload.get("signal_limit_progress_pct"),
        "intraday_order_flow_available": False,
        "unsupported_intraday_conditions": UNSUPPORTED_ORDER_FLOW_CONDITIONS,
    }


def _find_entry_position(
    store: DuckDBStore,
    candidate: _GrowthBoardCandidate,
    minutes: pd.DataFrame,
    window_dates: list[date],
    config: GrowthBoardSurgeConfig,
) -> tuple[PaperPosition, pd.DataFrame] | None:
    day_minutes = minutes[
        pd.to_datetime(minutes["trade_time"]).dt.date == candidate.trade_date
    ].sort_values("trade_time").reset_index(drop=True)
    if day_minutes.empty:
        return None
    if _is_intraday_yiziban(day_minutes, candidate.limit_up_price, config.price_tol):
        return None

    cum_vol = 0.0
    cum_amount = 0.0
    clocked_amount_history: list[tuple[time, float]] = []
    for idx, row in day_minutes.iterrows():
        quote_time = _as_datetime(row["trade_time"])
        minute_amount = float(row["amount"]) if pd.notna(row["amount"]) else 0.0
        minute_vol = float(row["vol"]) if pd.notna(row["vol"]) else 0.0
        if minute_vol > 0:
            cum_vol += minute_vol
        if minute_amount > 0:
            cum_amount += minute_amount
        vwap = cum_amount / cum_vol if cum_vol > 0 else None
        quote = _quote_from_close(row)
        if quote_time.time() < config.min_signal_time:
            clocked_amount_history.append((quote_time.time(), minute_amount))
            continue
        if quote.price >= candidate.limit_up_price - config.price_tol:
            clocked_amount_history.append((quote_time.time(), minute_amount))
            continue
        if (
            config.require_vwap_strength
            and vwap is not None
            and quote.price < vwap * (1 + config.vwap_buffer_pct)
        ):
            clocked_amount_history.append((quote_time.time(), minute_amount))
            continue

        raw_features = build_intraday_relative_volume_features(
            store,
            candidate.ts_code,
            quote_time,
            current_minute_amount=minute_amount,
            current_cum_amount=cum_amount,
            current_day_amounts=clocked_amount_history,
            lookback_days=config.lookback_days,
            freq=config.freq,
        )
        features = _extract_relative_volume_features(
            raw_features,
            config.lookback_days,
        )
        if not _passes_surge_filter(features, config):
            clocked_amount_history.append((quote_time.time(), minute_amount))
            continue

        execution_index = idx + 1
        if execution_index >= len(day_minutes):
            return None
        execution_row = day_minutes.iloc[execution_index]
        execution_quote = _quote_from_open(execution_row)
        if execution_quote.price >= candidate.limit_up_price - config.price_tol:
            return None
        execution_time = _as_datetime(execution_row["trade_time"])
        limit_progress = (
            (quote.price - candidate.pre_close)
            / (candidate.limit_up_price - candidate.pre_close)
            * 100
            if candidate.limit_up_price > candidate.pre_close
            else None
        )
        risk_payload = {
            **features,
            "signal_vwap": round(vwap, 4) if vwap is not None else None,
            "signal_limit_progress_pct": (
                round(limit_progress, 4) if limit_progress is not None else None
            ),
            "intraday_order_flow_available": False,
            "unsupported_intraday_conditions": UNSUPPORTED_ORDER_FLOW_CONDITIONS,
        }
        risk_plan = PaperRiskPlan(payload=risk_payload)
        watch_item = _WatchItem(
            ts_code=candidate.ts_code,
            pool="growth_board_surge",
            name=candidate.name,
            entry_date=candidate.trade_date,
            reference_date=candidate.previous_date,
            limit_up_date=candidate.trade_date,
            t_close=candidate.pre_close,
            t_high=None,
            limit_up_price_next=candidate.limit_up_price,
            stop_weak=0.0,
        )
        signal = {
            "level": "growth_board_volume_surge",
            "trigger_type": "growth_board_volume_surge",
            "level_price": candidate.limit_up_price,
            "signal_price": quote.price,
        }
        position = open_position_from_signal(
            watch_item,
            execution_quote,
            signal,
            execution_time,
            config.paper,
            earliest_exit_date=window_dates[1],
            risk_plan=risk_plan,
        )
        return position, day_minutes

    return None


def run_growth_board_surge_replay(
    store: DuckDBStore,
    *,
    start_date: str | date,
    end_date: str | date,
    config: GrowthBoardSurgeConfig | None = None,
) -> pd.DataFrame:
    """回放“科创/创业板 + 均线多头 + 分钟放量”入场与 T+1 离场。

    盘中入场只使用信号分钟及之前的数据；外盘/内盘和大单净量当前没有可
    历史回放的数据源，因此输出缺口字段，不参与过滤。
    """
    cfg = config or GrowthBoardSurgeConfig()
    start = _as_date(start_date)
    end = _as_date(end_date)
    calendar = _trading_dates(store)
    if not calendar:
        return pd.DataFrame()

    rows: list[dict[str, object]] = []
    for trading_date in [day for day in calendar if start <= day <= end]:
        previous_date = _previous_trading_date(calendar, trading_date)
        if previous_date is None:
            continue
        window_dates = _window_trading_dates(calendar, trading_date, cfg.max_hold_days)
        if len(window_dates) <= cfg.max_hold_days:
            continue
        _, window_end = _day_bounds(window_dates[-1])
        for candidate in _query_candidates(store, trading_date, previous_date):
            day_start, _ = _day_bounds(trading_date)
            minutes = store.query_minute_bars(
                candidate.ts_code,
                day_start,
                window_end,
                freq=cfg.freq,
            )
            if minutes.empty:
                continue
            entry = _find_entry_position(
                store,
                candidate,
                minutes,
                window_dates,
                cfg,
            )
            if entry is None:
                continue
            position, day_minutes = entry
            closed = _run_exit_scan(store, position, minutes, window_dates, cfg)
            if closed is None:
                continue
            rows.append(_position_to_row(closed, candidate, day_minutes))

    out = pd.DataFrame(rows)
    if not out.empty:
        out["intraday_order_flow_available"] = out[
            "intraday_order_flow_available"
        ].astype(object)
    return out
