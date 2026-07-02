"""基于历史分钟线的近似价量分布特征。"""

from __future__ import annotations

from datetime import date, datetime, time
from decimal import ROUND_FLOOR, ROUND_HALF_UP, Decimal

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

from rquant.paper import PaperRiskPlan
from rquant.storage.duckdb import DuckDBStore


class VolumeProfile(BaseModel):
    """分钟近似成交价量分布。"""

    model_config = ConfigDict(frozen=True)

    ts_code: str
    reference_date: date
    lookback_days: int
    start_date: date
    end_date: date
    rows_count: int
    total_vol: float
    total_amount: float
    vwap: float
    poc_price: float
    value_area_low: float
    value_area_high: float
    concentration_top5_pct: float
    below_reference_amount_pct: float
    above_reference_amount_pct: float
    source: str = "minute_bar"


class VolumeProfileRuleConfig(BaseModel):
    """价量分布参与入场与风控的规则参数。"""

    model_config = ConfigDict(frozen=True)

    enabled: bool = False
    filter_entry: bool = True
    require_profile: bool = True
    lookback_days: tuple[int, ...] = (90,)
    min_reclaimed_poc_count: int = Field(default=1, ge=1, le=1)
    min_reward_risk: float = Field(default=1.2, gt=0)
    max_stop_distance_pct: float = Field(default=0.045, gt=0, lt=0.2)
    min_take_profit_pct: float = Field(default=0.03, gt=0, lt=0.2)
    fallback_take_profit_pct: float = Field(default=0.05, gt=0, lt=0.3)
    support_buffer_pct: float = Field(default=0.003, ge=0, lt=0.05)
    resistance_buffer_pct: float = Field(default=0.002, ge=0, lt=0.05)
    trailing_stop_pct: float = Field(default=0.02, gt=0, lt=0.2)


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


def _lookback_dates(
    store: DuckDBStore,
    reference_date: date,
    lookback_days: int,
) -> list[date]:
    df = store._conn.execute(
        """
        SELECT DISTINCT trade_date
        FROM daily_bar
        WHERE trade_date < ?
        ORDER BY trade_date DESC
        LIMIT ?
        """,
        [reference_date, lookback_days],
    ).fetchdf()
    return sorted(_as_date(value) for value in df["trade_date"].tolist())


def _reference_price(
    store: DuckDBStore,
    ts_code: str,
    reference_date: date,
) -> float | None:
    row = store._conn.execute(
        """
        SELECT close
        FROM daily_bar
        WHERE ts_code = ?
          AND trade_date = ?
        """,
        [ts_code, reference_date],
    ).fetchone()
    if row is None or row[0] is None:
        return None
    return float(row[0])


def _adj_factor_ratios(
    store: DuckDBStore,
    ts_code: str,
    *,
    start_date: date,
    end_date: date,
    reference_date: date,
) -> dict[date, float]:
    df = store._conn.execute(
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
    if df.empty:
        return {}

    ref_rows = df[df["trade_date"].apply(_as_date) == reference_date]
    if ref_rows.empty or pd.isna(ref_rows.iloc[0]["adj_factor"]):
        return {}
    ref_factor = float(ref_rows.iloc[0]["adj_factor"])
    if ref_factor <= 0:
        return {}

    ratios: dict[date, float] = {}
    for _, row in df.iterrows():
        trade_date = _as_date(row["trade_date"])
        if trade_date < start_date or trade_date > end_date:
            continue
        factor = float(row["adj_factor"]) if pd.notna(row["adj_factor"]) else 0.0
        if factor > 0:
            ratios[trade_date] = factor / ref_factor
    return ratios


def _minute_trade_price(row: pd.Series) -> float:
    vol = float(row["vol"]) if pd.notna(row["vol"]) else 0.0
    amount = float(row["amount"]) if pd.notna(row["amount"]) else 0.0
    if vol > 0 and amount > 0:
        return amount / vol
    return float(row["close"])


def _round_price(value: float) -> float:
    return float(Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _floor_price(value: float) -> float:
    return float(Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_FLOOR))


def _value_area(bins: pd.DataFrame, ratio: float = 0.70) -> tuple[float, float]:
    ranked = bins.sort_values("amount", ascending=False).reset_index(drop=True)
    threshold = bins["amount"].sum() * ratio
    selected: list[float] = []
    amount_sum = 0.0
    for _, row in ranked.iterrows():
        selected.append(float(row["price_bin"]))
        amount_sum += float(row["amount"])
        if amount_sum >= threshold:
            break
    return min(selected), max(selected)


def calculate_volume_profile(
    store: DuckDBStore,
    ts_code: str,
    *,
    reference_date: date,
    lookback_days: int,
    freq: str = "1min",
    bin_pct: float = 0.005,
) -> VolumeProfile | None:
    """计算参考日前 N 个交易日的近似价量分布。"""
    dates = _lookback_dates(store, reference_date, lookback_days)
    if not dates:
        return None

    ref_price = _reference_price(store, ts_code, reference_date)
    if ref_price is None or ref_price <= 0:
        return None

    start = datetime.combine(dates[0], time(9, 30))
    end = datetime.combine(dates[-1], time(15, 0))
    minutes = store.query_minute_bars(ts_code, start, end, freq=freq)
    if minutes.empty:
        return None

    payload = minutes.copy()
    vol = pd.to_numeric(payload["vol"], errors="coerce").fillna(0.0)
    amount = pd.to_numeric(payload["amount"], errors="coerce").fillna(0.0)
    close = pd.to_numeric(payload["close"], errors="coerce")
    payload["trade_price"] = close
    valid_trade_amount = (vol > 0) & (amount > 0)
    payload.loc[valid_trade_amount, "trade_price"] = (
        amount.loc[valid_trade_amount] / vol.loc[valid_trade_amount]
    )
    ratios = _adj_factor_ratios(
        store,
        ts_code,
        start_date=dates[0],
        end_date=dates[-1],
        reference_date=reference_date,
    )
    if ratios:
        payload["trade_date"] = pd.to_datetime(payload["trade_time"]).dt.date
        payload["basis_ratio"] = payload["trade_date"].map(ratios).fillna(1.0)
        payload["trade_price"] = payload["trade_price"] * payload["basis_ratio"]
    bin_size = max(round(ref_price * bin_pct, 4), 0.01)
    payload["price_bin"] = (
        (payload["trade_price"] / bin_size).round() * bin_size
    ).round(2)
    bins = (
        payload.groupby("price_bin", as_index=False)
        .agg(amount=("amount", "sum"), vol=("vol", "sum"))
        .sort_values("price_bin")
    )
    if bins.empty:
        return None

    total_amount = float(payload["amount"].sum())
    total_vol = float(payload["vol"].sum())
    if total_amount <= 0 or total_vol <= 0:
        return None

    poc_row = bins.sort_values("amount", ascending=False).iloc[0]
    value_low, value_high = _value_area(bins)
    top5_amount = float(bins.sort_values("amount", ascending=False).head(5)["amount"].sum())
    below_amount = float(bins.loc[bins["price_bin"] < ref_price, "amount"].sum())
    above_amount = float(bins.loc[bins["price_bin"] > ref_price, "amount"].sum())

    return VolumeProfile(
        ts_code=ts_code,
        reference_date=reference_date,
        lookback_days=lookback_days,
        start_date=dates[0],
        end_date=dates[-1],
        rows_count=len(payload),
        total_vol=total_vol,
        total_amount=total_amount,
        vwap=total_amount / total_vol,
        poc_price=float(poc_row["price_bin"]),
        value_area_low=value_low,
        value_area_high=value_high,
        concentration_top5_pct=top5_amount / total_amount * 100,
        below_reference_amount_pct=below_amount / total_amount * 100,
        above_reference_amount_pct=above_amount / total_amount * 100,
    )


def scale_volume_profile(profile: VolumeProfile, ratio: float) -> VolumeProfile:
    """把价量分布价格字段缩放到新的价格基准。"""
    if ratio <= 0 or ratio == 1:
        return profile
    return profile.model_copy(update={
        "vwap": profile.vwap * ratio,
        "poc_price": profile.poc_price * ratio,
        "value_area_low": profile.value_area_low * ratio,
        "value_area_high": profile.value_area_high * ratio,
    })


def _support_candidates(
    profiles: list[VolumeProfile],
    entry_price: float,
) -> list[tuple[str, float]]:
    candidates: list[tuple[str, float]] = []
    for profile in profiles:
        prefix = f"vp{profile.lookback_days}"
        for label, value in (
            ("value_high", profile.value_area_high),
            ("poc", profile.poc_price),
            ("vwap", profile.vwap),
            ("value_low", profile.value_area_low),
        ):
            if 0 < value < entry_price:
                candidates.append((f"{prefix}_{label}", float(value)))
    return candidates


def _resistance_candidates(
    profiles: list[VolumeProfile],
    entry_price: float,
) -> list[tuple[str, float]]:
    candidates: list[tuple[str, float]] = []
    for profile in profiles:
        prefix = f"vp{profile.lookback_days}"
        for label, value in (
            ("value_low", profile.value_area_low),
            ("vwap", profile.vwap),
            ("poc", profile.poc_price),
            ("value_high", profile.value_area_high),
        ):
            if value > entry_price:
                candidates.append((f"{prefix}_{label}", float(value)))
    return candidates


def build_volume_profile_risk_plan(
    profiles: list[VolumeProfile],
    *,
    entry_price: float,
    config: VolumeProfileRuleConfig,
) -> PaperRiskPlan:
    """用 90 日价量分布生成入场过滤、止损和止盈计划。"""
    if not config.enabled:
        return PaperRiskPlan(entry_allowed=True)

    valid_profiles = [profile for profile in profiles if profile.rows_count > 0]
    if not valid_profiles:
        if not config.require_profile:
            return PaperRiskPlan(entry_allowed=True)
        return PaperRiskPlan(
            entry_allowed=False,
            reject_reason="missing_volume_profile",
            payload={"reject_reason": "missing_volume_profile"},
        )

    reclaimed_poc_count = sum(
        1 for profile in valid_profiles if entry_price >= profile.poc_price
    )
    lookbacks_used = [profile.lookback_days for profile in valid_profiles]
    payload: dict[str, object] = {
        "lookbacks_used": lookbacks_used,
        "reclaimed_poc_count": reclaimed_poc_count,
        "profiles_count": len(valid_profiles),
    }
    if (
        config.filter_entry
        and reclaimed_poc_count < min(config.min_reclaimed_poc_count, len(valid_profiles))
    ):
        payload["reject_reason"] = "below_major_poc"
        return PaperRiskPlan(
            entry_allowed=False,
            reject_reason="below_major_poc",
            payload=payload,
        )

    supports = _support_candidates(valid_profiles, entry_price)
    if supports:
        support_basis, support_price = max(supports, key=lambda candidate: candidate[1])
        support_stop = support_price * (1 - config.support_buffer_pct)
    else:
        support_basis = "profile_pct_cap"
        support_stop = entry_price * (1 - config.max_stop_distance_pct)

    capped_stop = max(
        support_stop,
        entry_price * (1 - config.max_stop_distance_pct),
    )
    if capped_stop >= entry_price:
        capped_stop = entry_price * 0.995

    resistances = sorted(
        _resistance_candidates(valid_profiles, entry_price),
        key=lambda candidate: candidate[1],
    )
    min_target_price = entry_price * (1 + config.min_take_profit_pct)
    close_resistance = next(
        (
            (basis, price)
            for basis, price in resistances
            if entry_price < price < min_target_price
        ),
        None,
    )
    if config.filter_entry and close_resistance is not None:
        payload.update({
            "reject_reason": "overhead_resistance_too_close",
            "resistance_basis": close_resistance[0],
            "resistance_price": close_resistance[1],
        })
        return PaperRiskPlan(
            entry_allowed=False,
            reject_reason="overhead_resistance_too_close",
            payload=payload,
        )

    target = next(
        ((basis, price) for basis, price in resistances if price >= min_target_price),
        None,
    )
    if target is None:
        take_profit_basis = "profile_fallback_pct"
        take_profit_price = entry_price * (1 + config.fallback_take_profit_pct)
    else:
        take_profit_basis = target[0]
        take_profit_price = target[1] * (1 - config.resistance_buffer_pct)

    risk = entry_price - capped_stop
    reward = take_profit_price - entry_price
    reward_risk = reward / risk if risk > 0 else 0
    payload.update({
        "support_basis": support_basis,
        "support_price": _round_price(support_stop),
        "stop_loss_price": _round_price(capped_stop),
        "take_profit_basis": take_profit_basis,
        "take_profit_price": _round_price(take_profit_price),
        "reward_risk": round(reward_risk, 4),
    })
    if config.filter_entry and reward_risk < config.min_reward_risk:
        payload["reject_reason"] = "reward_risk_too_low"
        return PaperRiskPlan(
            entry_allowed=False,
            reject_reason="reward_risk_too_low",
            stop_loss_price=_round_price(capped_stop),
            stop_loss_basis=support_basis,
            take_profit_price=_round_price(take_profit_price),
            take_profit_basis=take_profit_basis,
            trailing_stop_pct=config.trailing_stop_pct,
            payload=payload,
        )

    return PaperRiskPlan(
        entry_allowed=True,
        stop_loss_price=_round_price(capped_stop),
        stop_loss_basis=support_basis,
        take_profit_price=_floor_price(take_profit_price),
        take_profit_basis=take_profit_basis,
        trailing_stop_pct=config.trailing_stop_pct,
        payload=payload,
    )
