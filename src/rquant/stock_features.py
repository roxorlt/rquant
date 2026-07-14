"""候选股票日线位置、吸筹代理和分钟相对放量特征。"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime, time
from math import isfinite
from typing import Literal, Self

import pandas as pd
from pydantic import BaseModel, ConfigDict, model_validator

from rquant.price_adjustment import PriceFactorBasis, resolve_price_factor_basis
from rquant.storage.duckdb import DuckDBStore

_MA_ALIGNMENT_WINDOWS = (5, 10, 20, 60)
_PRICE_PERCENTILE_LOOKBACK = 250


class StockFeatureSnapshot(BaseModel):
    """一只股票在某个 as-of 时点可观察到的候选特征。"""

    model_config = ConfigDict(frozen=True)

    ts_code: str
    reference_date: date
    payload: dict[str, float | int | None]


class PriceBasisDiagnostic(BaseModel):
    """一个特征窗口的复权基准可用性。"""

    model_config = ConfigDict(frozen=True)

    feature_family: str
    window_days: int
    available: bool
    reason: str | None = None
    basis: PriceFactorBasis | None = None

    @model_validator(mode="after")
    def validate_state(self) -> Self:
        if self.available:
            if self.reason is not None:
                raise ValueError("available diagnostic cannot carry reason")
            if self.basis is not None and not self.basis.available:
                raise ValueError("available diagnostic cannot carry unavailable basis")
            return self
        if self.reason is None:
            raise ValueError("unavailable diagnostic requires reason")
        if self.basis is not None and self.basis.available:
            raise ValueError("unavailable diagnostic cannot carry available basis")
        return self


class DailyStockFeatureResult(BaseModel):
    """日线候选特征及各价格窗口的可用性诊断。"""

    model_config = ConfigDict(frozen=True)

    ts_code: str
    reference_date: date
    status: Literal["available", "partial", "no_data"]
    reason: str | None = None
    features: dict[str, float | int | None]
    diagnostics: dict[str, PriceBasisDiagnostic]

    @model_validator(mode="after")
    def validate_state(self) -> Self:
        if self.status == "available" and self.reason is not None:
            raise ValueError("available feature result cannot carry reason")
        if self.status != "available" and self.reason is None:
            raise ValueError("non-available feature result requires reason")
        return self


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


def _round_optional(value: float | None, digits: int = 4) -> float | None:
    if value is None or not isfinite(value):
        return None
    return round(float(value), digits)


def _query_daily_window(
    store: DuckDBStore,
    ts_code: str,
    reference_date: date,
    rows: int,
    *,
    include_reference: bool,
) -> pd.DataFrame:
    comparator = "<=" if include_reference else "<"
    df = store._conn.execute(
        f"""
        SELECT ts_code, trade_date, open, high, low, close, pre_close,
               pct_chg, vol, amount
        FROM daily_bar
        WHERE ts_code = ?
          AND trade_date {comparator} ?
        ORDER BY trade_date DESC
        LIMIT ?
        """,
        [ts_code, reference_date, rows],
    ).fetchdf()
    if df.empty:
        return df
    return df.sort_values("trade_date").reset_index(drop=True)


def _query_factor_map(
    store: DuckDBStore,
    ts_code: str,
    start_date: date,
    reference_date: date,
) -> dict[date, float | None]:
    factor_df = store._conn.execute(
        """
        SELECT trade_date, adj_factor
        FROM adj_factor
        WHERE ts_code = ?
          AND trade_date >= ?
          AND trade_date <= ?
        ORDER BY trade_date
        """,
        [ts_code, start_date, reference_date],
    ).fetchdf()
    return {
        _as_date(row["trade_date"]): (
            None if pd.isna(row["adj_factor"]) else float(row["adj_factor"])
        )
        for _, row in factor_df.iterrows()
    }


def _resolve_window_basis(
    df: pd.DataFrame,
    factor_map: dict[date, float | None],
    reference_date: date,
) -> PriceFactorBasis:
    return resolve_price_factor_basis(
        required_dates=tuple(df["trade_date"].apply(_as_date).tolist()),
        factor_by_date=factor_map,
        reference_date=reference_date,
    )


def _apply_price_basis(df: pd.DataFrame, basis: PriceFactorBasis) -> pd.DataFrame:
    if not basis.available:
        raise ValueError("cannot adjust prices with an unavailable basis")
    out = df.copy()
    out["trade_date_obj"] = out["trade_date"].apply(_as_date)
    out["basis_ratio"] = out["trade_date_obj"].map(basis.ratio_by_date())
    for col in ["open", "high", "low", "close", "pre_close"]:
        out[col] = pd.to_numeric(out[col], errors="coerce") * out["basis_ratio"]
    return out.drop(columns=["trade_date_obj", "basis_ratio"])


def _basis_diagnostic(
    feature_family: str,
    window_days: int,
    basis: PriceFactorBasis,
) -> PriceBasisDiagnostic:
    return PriceBasisDiagnostic(
        feature_family=feature_family,
        window_days=window_days,
        available=basis.available,
        reason=basis.unavailable_reason,
        basis=basis,
    )


def _insufficient_history_diagnostic(
    feature_family: str,
    window_days: int,
) -> PriceBasisDiagnostic:
    return PriceBasisDiagnostic(
        feature_family=feature_family,
        window_days=window_days,
        available=False,
        reason="insufficient_history",
    )


def _unavailable_price_position_features(
    lookback: int,
    actual_days: int,
) -> dict[str, float | int | None]:
    prefix = f"{lookback}d"
    return {
        f"price_window_days_{prefix}": actual_days,
        f"price_position_{prefix}_pct": None,
        f"price_rank_{prefix}_pct": None,
        f"distance_to_high_{prefix}_pct": None,
        f"distance_to_low_{prefix}_pct": None,
    }


def _price_position_features(
    df: pd.DataFrame,
    lookbacks: Iterable[int],
) -> dict[str, float | int | None]:
    out: dict[str, float | int | None] = {}
    if df.empty:
        return out
    max_lookback = max(lookbacks)
    payload = df.tail(max_lookback).copy()
    ref_close = float(payload.iloc[-1]["close"])

    for lookback in lookbacks:
        window = payload.tail(lookback).copy()
        closes = pd.to_numeric(window["close"], errors="coerce").dropna()
        highs = pd.to_numeric(window["high"], errors="coerce").dropna()
        lows = pd.to_numeric(window["low"], errors="coerce").dropna()
        prefix = f"{lookback}d"
        out[f"price_window_days_{prefix}"] = int(len(closes))
        if closes.empty or ref_close <= 0:
            out[f"price_position_{prefix}_pct"] = None
            out[f"price_rank_{prefix}_pct"] = None
            out[f"distance_to_high_{prefix}_pct"] = None
            out[f"distance_to_low_{prefix}_pct"] = None
            continue

        min_close = float(closes.min())
        max_close = float(closes.max())
        if max_close > min_close:
            position = (ref_close - min_close) / (max_close - min_close) * 100
        else:
            position = 50.0
        rank = float((closes <= ref_close).mean() * 100)
        max_high = float(highs.max()) if not highs.empty else ref_close
        min_low = float(lows.min()) if not lows.empty else ref_close
        out[f"price_position_{prefix}_pct"] = _round_optional(position)
        out[f"price_rank_{prefix}_pct"] = _round_optional(rank)
        out[f"distance_to_high_{prefix}_pct"] = _round_optional(
            (max_high / ref_close - 1) * 100 if ref_close > 0 else None
        )
        out[f"distance_to_low_{prefix}_pct"] = _round_optional(
            (ref_close / min_low - 1) * 100 if min_low > 0 else None
        )
    return out


def _accumulation_features(
    df: pd.DataFrame,
    lookback: int,
    *,
    obv_close: pd.Series | None,
) -> dict[str, float | int | None]:
    prefix = f"{lookback}d"
    if df.empty:
        return {
            f"accum_window_days_{prefix}": 0,
            f"accum_obv_change_{prefix}_pct": None,
            f"accum_ad_flow_{prefix}_pct": None,
            f"accum_up_down_amount_ratio_{prefix}": None,
            f"accum_heavy_no_drop_days_{prefix}": 0,
            f"accum_close_position_avg_{prefix}_pct": None,
        }

    payload = df.tail(lookback).copy()
    close = pd.to_numeric(payload["close"], errors="coerce")
    high = pd.to_numeric(payload["high"], errors="coerce")
    low = pd.to_numeric(payload["low"], errors="coerce")
    vol = pd.to_numeric(payload["vol"], errors="coerce").fillna(0.0)
    amount = pd.to_numeric(payload["amount"], errors="coerce").fillna(0.0)
    pct_chg = pd.to_numeric(payload["pct_chg"], errors="coerce").fillna(0.0)

    total_vol = float(vol.abs().sum())
    obv_change: float | None = None
    if obv_close is not None:
        adjusted_close = pd.to_numeric(obv_close, errors="coerce")
        direction = adjusted_close.diff().fillna(0.0).map(
            lambda value: 1 if value > 0 else -1 if value < 0 else 0
        )
        obv = (direction * vol).cumsum()
        if total_vol > 0 and len(obv) > 1:
            obv_change = (
                float(obv.iloc[-1]) - float(obv.iloc[0])
            ) / total_vol * 100

    price_range = (high - low).replace(0, pd.NA)
    close_position = ((close - low) / price_range).clip(lower=0, upper=1)
    money_flow_multiplier = ((2 * close - high - low) / price_range).fillna(0.0)
    ad_flow = (
        float((money_flow_multiplier * vol).sum()) / total_vol * 100
        if total_vol > 0
        else None
    )

    up_amount = float(amount[pct_chg > 0].sum())
    down_amount = float(amount[pct_chg < 0].sum())
    up_down_ratio = up_amount / down_amount if down_amount > 0 else None
    heavy_threshold = float(amount.median()) * 1.5 if len(amount) else 0.0
    heavy_no_drop_days = int(((amount >= heavy_threshold) & (pct_chg >= -1)).sum())

    return {
        f"accum_window_days_{prefix}": int(len(payload)),
        f"accum_obv_change_{prefix}_pct": _round_optional(obv_change),
        f"accum_ad_flow_{prefix}_pct": _round_optional(ad_flow),
        f"accum_up_down_amount_ratio_{prefix}": _round_optional(up_down_ratio),
        f"accum_heavy_no_drop_days_{prefix}": heavy_no_drop_days,
        f"accum_close_position_avg_{prefix}_pct": _round_optional(
            float(close_position.mean()) * 100 if not close_position.dropna().empty else None
        ),
    }


def _trend_features(df: pd.DataFrame) -> dict[str, float | int | None]:
    """均线多头排列与 250 日收盘百分位。"""
    out: dict[str, float | int | None] = {
        "ma_alignment": None,
        "price_percentile_250d": None,
    }
    if df.empty:
        return out
    closes = pd.to_numeric(df["close"], errors="coerce").dropna()
    if len(closes) >= max(_MA_ALIGNMENT_WINDOWS):
        mas = [float(closes.tail(window).mean()) for window in _MA_ALIGNMENT_WINDOWS]
        aligned = all(fast > slow for fast, slow in zip(mas, mas[1:], strict=False))
        out["ma_alignment"] = 1 if aligned else 0
    if len(closes) >= _PRICE_PERCENTILE_LOOKBACK:
        window = closes.tail(_PRICE_PERCENTILE_LOOKBACK)
        ref_close = float(window.iloc[-1])
        out["price_percentile_250d"] = _round_optional(float((window <= ref_close).mean()))
    return out


def build_daily_stock_feature_result(
    store: DuckDBStore,
    ts_code: str,
    reference_date: date,
    *,
    price_lookbacks: tuple[int, ...] = (90, 120, 250),
    accumulation_lookback: int = 20,
) -> DailyStockFeatureResult:
    """生成 T 日特征，并保留每个实际价格窗口的复权诊断。"""
    trend_rows = max(max(_MA_ALIGNMENT_WINDOWS), _PRICE_PERCENTILE_LOOKBACK)
    max_rows = max(max(price_lookbacks), accumulation_lookback + 1, trend_rows)
    daily = _query_daily_window(
        store,
        ts_code,
        reference_date,
        max_rows,
        include_reference=True,
    )
    if daily.empty:
        return DailyStockFeatureResult(
            ts_code=ts_code,
            reference_date=reference_date,
            status="no_data",
            reason="missing_daily_data",
            features={},
            diagnostics={},
        )

    factor_map = _query_factor_map(
        store,
        ts_code,
        _as_date(daily.iloc[0]["trade_date"]),
        reference_date,
    )
    features: dict[str, float | int | None] = {}
    diagnostics: dict[str, PriceBasisDiagnostic] = {}

    for lookback in price_lookbacks:
        window = daily.tail(lookback).copy()
        basis = _resolve_window_basis(window, factor_map, reference_date)
        key = f"price_position_{lookback}d"
        diagnostics[key] = _basis_diagnostic(key, lookback, basis)
        if basis.available:
            features.update(
                _price_position_features(
                    _apply_price_basis(window, basis),
                    (lookback,),
                )
            )
        else:
            features.update(
                _unavailable_price_position_features(lookback, len(window))
            )

    ma_window_days = max(_MA_ALIGNMENT_WINDOWS)
    ma_key = f"ma_alignment_{ma_window_days}d"
    features["ma_alignment"] = None
    ma_window = daily.tail(ma_window_days).copy()
    if len(ma_window) < ma_window_days:
        diagnostics[ma_key] = _insufficient_history_diagnostic(
            ma_key,
            ma_window_days,
        )
    else:
        ma_basis = _resolve_window_basis(ma_window, factor_map, reference_date)
        diagnostics[ma_key] = _basis_diagnostic(
            ma_key,
            ma_window_days,
            ma_basis,
        )
        if ma_basis.available:
            features["ma_alignment"] = _trend_features(
                _apply_price_basis(ma_window, ma_basis)
            )["ma_alignment"]

    percentile_key = f"price_percentile_{_PRICE_PERCENTILE_LOOKBACK}d"
    features["price_percentile_250d"] = None
    percentile_window = daily.tail(_PRICE_PERCENTILE_LOOKBACK).copy()
    if len(percentile_window) < _PRICE_PERCENTILE_LOOKBACK:
        diagnostics[percentile_key] = _insufficient_history_diagnostic(
            percentile_key,
            _PRICE_PERCENTILE_LOOKBACK,
        )
    else:
        percentile_basis = _resolve_window_basis(
            percentile_window,
            factor_map,
            reference_date,
        )
        diagnostics[percentile_key] = _basis_diagnostic(
            percentile_key,
            _PRICE_PERCENTILE_LOOKBACK,
            percentile_basis,
        )
        if percentile_basis.available:
            features["price_percentile_250d"] = _trend_features(
                _apply_price_basis(percentile_window, percentile_basis)
            )["price_percentile_250d"]

    before_reference = daily[
        daily["trade_date"].apply(_as_date) < reference_date
    ].tail(accumulation_lookback).copy()
    accumulation_key = f"accumulation_obv_{accumulation_lookback}d"
    scale_invariant_key = f"accumulation_scale_invariant_{accumulation_lookback}d"
    adjusted_obv_close: pd.Series | None = None
    if before_reference.empty:
        diagnostics[accumulation_key] = _insufficient_history_diagnostic(
            accumulation_key,
            accumulation_lookback,
        )
        diagnostics[scale_invariant_key] = _insufficient_history_diagnostic(
            scale_invariant_key,
            accumulation_lookback,
        )
    else:
        diagnostics[scale_invariant_key] = PriceBasisDiagnostic(
            feature_family=scale_invariant_key,
            window_days=accumulation_lookback,
            available=True,
        )
        accumulation_basis = _resolve_window_basis(
            before_reference,
            factor_map,
            reference_date,
        )
        diagnostics[accumulation_key] = _basis_diagnostic(
            accumulation_key,
            accumulation_lookback,
            accumulation_basis,
        )
        if accumulation_basis.available:
            adjusted_obv_close = _apply_price_basis(
                before_reference,
                accumulation_basis,
            )["close"]
    features.update(
        _accumulation_features(
            before_reference,
            accumulation_lookback,
            obv_close=adjusted_obv_close,
        )
    )

    has_unavailable_basis = any(
        diagnostic.basis is not None and not diagnostic.basis.available
        for diagnostic in diagnostics.values()
    )
    has_unavailable_diagnostic = any(
        not diagnostic.available for diagnostic in diagnostics.values()
    )
    if has_unavailable_basis:
        status = "partial"
        reason = "price_basis_unavailable"
    elif has_unavailable_diagnostic:
        status = "partial"
        reason = "insufficient_history"
    else:
        status = "available"
        reason = None
    return DailyStockFeatureResult(
        ts_code=ts_code,
        reference_date=reference_date,
        status=status,
        reason=reason,
        features=features,
        diagnostics=diagnostics,
    )


def build_daily_stock_features(
    store: DuckDBStore,
    ts_code: str,
    reference_date: date,
    *,
    price_lookbacks: tuple[int, ...] = (90, 120, 250),
    accumulation_lookback: int = 20,
) -> dict[str, float | int | None]:
    """兼容接口：仅返回数值特征，不把诊断字符串混入下游。"""
    return build_daily_stock_feature_result(
        store,
        ts_code,
        reference_date,
        price_lookbacks=price_lookbacks,
        accumulation_lookback=accumulation_lookback,
    ).features


def build_intraday_relative_volume_features(
    store: DuckDBStore,
    ts_code: str,
    signal_time: datetime,
    *,
    current_minute_amount: float,
    current_cum_amount: float,
    current_day_amounts: Iterable[tuple[time, float]] | None = None,
    lookback_days: int = 20,
    freq: str = "1min",
) -> dict[str, float | int | None]:
    """比较当前信号分钟与过去同一时刻的成交额基准。

    只使用 signal_time 日期之前的分钟数据，避免把当天后续分钟带进来。
    """
    signal_date = signal_time.date()
    signal_clock = signal_time.time()
    opening_segment_end = time(9, 32)
    is_opening_segment = signal_clock <= opening_segment_end
    day_amounts = list(current_day_amounts or [])
    regular_prior_amounts = [
        float(amount)
        for clock, amount in day_amounts
        if opening_segment_end < clock < signal_clock and amount > 0
    ]

    def _accel(window: int) -> float | None:
        if is_opening_segment:
            return None
        prior = regular_prior_amounts[-window:]
        if not prior:
            return None
        baseline = float(pd.Series(prior).median())
        return current_minute_amount / baseline if baseline > 0 else None

    intraday_features: dict[str, float | int | None] = {
        "signal_opening_segment": 1 if is_opening_segment else 0,
        "signal_opening_segment_amount": (
            _round_optional(current_cum_amount) if is_opening_segment else None
        ),
        "signal_amount_accel_5m": _round_optional(_accel(5)),
        "signal_amount_accel_10m": _round_optional(_accel(10)),
    }
    previous_dates_df = store._conn.execute(
        """
        SELECT DISTINCT CAST(trade_time AS DATE) AS trade_date
        FROM minute_bar
        WHERE ts_code = ?
          AND freq = ?
          AND CAST(trade_time AS DATE) < ?
        ORDER BY trade_date DESC
        LIMIT ?
        """,
        [ts_code, freq, signal_date, lookback_days],
    ).fetchdf()
    prefix = f"{lookback_days}d"
    if previous_dates_df.empty:
        return {
            "signal_minute_amount": _round_optional(current_minute_amount),
            "signal_cum_amount_asof": _round_optional(current_cum_amount),
            f"hist_same_minute_amount_median_{prefix}": None,
            f"hist_cum_amount_asof_median_{prefix}": None,
            f"signal_rel_amount_same_minute_{prefix}": None,
            f"signal_rel_cum_amount_asof_{prefix}": None,
            f"hist_intraday_days_{prefix}": 0,
            **intraday_features,
        }

    previous_dates = [_as_date(value) for value in previous_dates_df["trade_date"].tolist()]
    start = datetime.combine(min(previous_dates), time(9, 30))
    end = datetime.combine(max(previous_dates), signal_clock)
    minutes = store.query_minute_bars(ts_code, start, end, freq=freq)
    if minutes.empty:
        hist_days = 0
        same_median = None
        cum_median = None
    else:
        payload = minutes.copy()
        payload["trade_date"] = pd.to_datetime(payload["trade_time"]).dt.date
        payload["clock"] = pd.to_datetime(payload["trade_time"]).dt.time
        payload = payload[payload["trade_date"].isin(previous_dates)]
        payload = payload[payload["clock"] <= signal_clock]
        same_minute = payload[payload["clock"] == signal_clock]
        cum_by_day = payload.groupby("trade_date")["amount"].sum()
        same_median = (
            float(pd.to_numeric(same_minute["amount"], errors="coerce").median())
            if not same_minute.empty
            else None
        )
        cum_median = float(cum_by_day.median()) if not cum_by_day.empty else None
        hist_days = int(len(cum_by_day))

    rel_same = (
        current_minute_amount / same_median
        if same_median is not None and same_median > 0
        else None
    )
    rel_cum = (
        current_cum_amount / cum_median
        if cum_median is not None and cum_median > 0
        else None
    )
    return {
        "signal_minute_amount": _round_optional(current_minute_amount),
        "signal_cum_amount_asof": _round_optional(current_cum_amount),
        f"hist_same_minute_amount_median_{prefix}": _round_optional(same_median),
        f"hist_cum_amount_asof_median_{prefix}": _round_optional(cum_median),
        f"signal_rel_amount_same_minute_{prefix}": _round_optional(rel_same),
        f"signal_rel_cum_amount_asof_{prefix}": _round_optional(rel_cum),
        f"hist_intraday_days_{prefix}": hist_days,
        **intraday_features,
    }
