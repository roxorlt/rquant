"""基于历史分钟线的近似价量分布特征。"""

from __future__ import annotations

from datetime import date, datetime, time
from decimal import ROUND_FLOOR, ROUND_HALF_UP, Decimal
from typing import Literal, Self

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.paper import PaperRiskPlan
from rquant.price_adjustment import PriceFactorBasis, resolve_price_factor_basis
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
    total_vol: float = Field(description="reference_date 基准的可比股数")
    total_amount: float = Field(description="未经复权的原始成交额")
    vwap: float
    poc_price: float
    value_area_low: float
    value_area_high: float
    concentration_top5_pct: float
    below_reference_amount_pct: float = Field(description="原始成交额口径")
    above_reference_amount_pct: float = Field(description="原始成交额口径")
    below_reference_volume_pct: float = Field(description="可比股数口径")
    above_reference_volume_pct: float = Field(description="可比股数口径")
    weight_basis: Literal["adjusted_share_volume"] = "adjusted_share_volume"
    source: str = "minute_bar"


VolumeProfileCalculationStatus = Literal[
    "available",
    "no_data",
    "invalid_data",
    "price_basis_unavailable",
]


class VolumeProfileCalculation(BaseModel):
    """价量分布计算结果及不可用诊断。"""

    model_config = ConfigDict(frozen=True)

    status: VolumeProfileCalculationStatus
    reason: str | None = None
    profile: VolumeProfile | None = None
    price_basis: PriceFactorBasis | None = None

    @model_validator(mode="after")
    def validate_state(self) -> Self:
        if self.status == "available":
            if self.profile is None or self.reason is not None:
                raise ValueError("available calculation requires profile without reason")
            if self.price_basis is None or not self.price_basis.available:
                raise ValueError("available calculation requires available price basis")
            return self
        if self.profile is not None or self.reason is None:
            raise ValueError("unavailable calculation requires reason without profile")
        if self.status == "price_basis_unavailable" and (
            self.price_basis is None or self.price_basis.available
        ):
            raise ValueError("price_basis_unavailable requires unavailable price basis")
        return self


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
    bin_ratio: float | None = Field(
        default=None,
        ge=0,
        lt=0.1,
        description="自适应分桶相对宽度；None/0 表示沿用历史 bin_pct 口径，不改变现有行为",
    )


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


def _adj_factor_basis(
    store: DuckDBStore,
    ts_code: str,
    *,
    required_dates: set[date],
    reference_date: date,
) -> PriceFactorBasis:
    df = store._conn.execute(
        """
        SELECT trade_date, adj_factor
        FROM adj_factor
        WHERE ts_code = ?
          AND trade_date >= ?
          AND trade_date <= ?
        ORDER BY trade_date
        """,
        [ts_code, min(required_dates), reference_date],
    ).fetchdf()
    factor_by_date = {
        _as_date(row["trade_date"]): (
            None if pd.isna(row["adj_factor"]) else float(row["adj_factor"])
        )
        for _, row in df.iterrows()
    }
    return resolve_price_factor_basis(
        required_dates=required_dates,
        factor_by_date=factor_by_date,
        reference_date=reference_date,
    )


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


def _resolve_bin_size(
    reference_price: float,
    *,
    bin_pct: float,
    bin_ratio: float | None,
) -> float:
    """价格分桶宽度：bin_ratio 显式给值时走 VP 规格口径，否则保持历史 bin_pct 口径。"""
    if bin_ratio:
        return max(0.01, round(reference_price * bin_ratio, 2))
    return max(round(reference_price * bin_pct, 4), 0.01)


def _poc_index(
    prices: list[float],
    weights: list[float],
    reference_price: float | None,
) -> int:
    """成交量最大的 bin；并列时取离参考价最近的那个，仍并列取低价侧，保证可复现。"""
    peak = max(weights)
    tied = [index for index, weight in enumerate(weights) if weight == peak]
    if len(tied) == 1:
        return tied[0]
    anchor = reference_price if reference_price is not None else prices[tied[0]]
    return min(tied, key=lambda index: (abs(prices[index] - anchor), prices[index]))


def _value_area(
    bins: pd.DataFrame,
    *,
    weight_column: str,
    ratio: float = 0.70,
    reference_price: float | None = None,
) -> tuple[float, float]:
    """标准 Market Profile 价值区：从 POC 逐 bin 向成交量更大的一侧连续扩展至 ratio。

    价值区必须是连续区间：旧实现按成交量降序取 top-N 再取 min/max，双峰分布下会把
    两个峰之间的低成交区一并圈进来，系统性拉宽 VA（VP 规格 §3.3.4 缺陷 G20）。
    """
    ordered = bins.sort_values("price_bin")
    prices = [float(value) for value in ordered["price_bin"]]
    weights = [float(value) for value in ordered[weight_column]]
    if not prices:
        msg = "value area requires at least one price bin"
        raise ValueError(msg)

    poc = _poc_index(prices, weights, reference_price)
    threshold = sum(weights) * ratio
    low = high = poc
    covered = weights[poc]
    last = len(prices) - 1
    while covered < threshold and (low > 0 or high < last):
        upper = weights[high + 1] if high < last else None
        lower = weights[low - 1] if low > 0 else None
        # 两侧成交量并列时固定向上扩，避免结果依赖遍历顺序
        if lower is None or (upper is not None and upper >= lower):
            high += 1
            covered += weights[high]
        else:
            low -= 1
            covered += weights[low]
    return prices[low], prices[high]


def calculate_volume_profile_outcome(
    store: DuckDBStore,
    ts_code: str,
    *,
    reference_date: date,
    lookback_days: int,
    freq: str = "1min",
    bin_pct: float = 0.005,
    bin_ratio: float | None = None,
) -> VolumeProfileCalculation:
    """计算参考日前 N 个交易日的价量分布，并保留失败诊断。"""
    dates = _lookback_dates(store, reference_date, lookback_days)
    if not dates:
        return VolumeProfileCalculation(status="no_data", reason="no_trading_dates")

    ref_price = _reference_price(store, ts_code, reference_date)
    if ref_price is None or ref_price <= 0:
        return VolumeProfileCalculation(
            status="invalid_data",
            reason="missing_or_invalid_reference_price",
        )

    start = datetime.combine(dates[0], time(9, 30))
    end = datetime.combine(dates[-1], time(15, 0))
    minutes = store.query_minute_bars(ts_code, start, end, freq=freq)
    if minutes.empty:
        return VolumeProfileCalculation(status="no_data", reason="missing_minute_data")

    payload = minutes.copy()
    vol = pd.to_numeric(payload["vol"], errors="coerce").fillna(0.0)
    amount = pd.to_numeric(payload["amount"], errors="coerce").fillna(0.0)
    close = pd.to_numeric(payload["close"], errors="coerce")
    payload["raw_trade_price"] = close
    valid_trade_amount = (vol > 0) & (amount > 0)
    payload.loc[valid_trade_amount, "raw_trade_price"] = (
        amount.loc[valid_trade_amount] / vol.loc[valid_trade_amount]
    )
    payload["trade_date"] = pd.to_datetime(payload["trade_time"]).dt.date
    minute_dates = set(payload["trade_date"].tolist())
    price_basis = _adj_factor_basis(
        store,
        ts_code,
        required_dates=minute_dates,
        reference_date=reference_date,
    )
    if not price_basis.available:
        return VolumeProfileCalculation(
            status="price_basis_unavailable",
            reason=price_basis.unavailable_reason,
            price_basis=price_basis,
        )
    payload["basis_ratio"] = payload["trade_date"].map(
        price_basis.ratio_by_date()
    )
    if payload["basis_ratio"].isna().any():
        return VolumeProfileCalculation(
            status="invalid_data",
            reason="unmapped_price_basis_ratio",
            price_basis=price_basis,
        )
    payload["qfq_price"] = payload["raw_trade_price"] * payload["basis_ratio"]
    payload["comparable_volume"] = vol / payload["basis_ratio"]
    bin_size = _resolve_bin_size(ref_price, bin_pct=bin_pct, bin_ratio=bin_ratio)
    payload["price_bin"] = (
        (payload["qfq_price"] / bin_size).round() * bin_size
    ).round(2)
    bins = (
        payload.groupby("price_bin", as_index=False)
        .agg(
            amount=("amount", "sum"),
            comparable_volume=("comparable_volume", "sum"),
        )
        .sort_values("price_bin")
        .reset_index(drop=True)
    )
    if bins.empty:
        return VolumeProfileCalculation(status="no_data", reason="empty_price_bins")

    total_amount = float(payload["amount"].sum())
    total_vol = float(payload["comparable_volume"].sum())
    if total_amount <= 0 or total_vol <= 0:
        return VolumeProfileCalculation(
            status="invalid_data",
            reason="non_positive_profile_totals",
            price_basis=price_basis,
        )
    adjusted_vwap = float(
        (payload["qfq_price"] * payload["comparable_volume"]).sum()
    ) / total_vol

    # POC 与价值区共用同一套并列裁决，保证 value_low ≤ poc_price ≤ value_high 恒成立
    bin_prices = [float(value) for value in bins["price_bin"]]
    bin_weights = [float(value) for value in bins["comparable_volume"]]
    poc_price = bin_prices[_poc_index(bin_prices, bin_weights, ref_price)]
    value_low, value_high = _value_area(
        bins,
        weight_column="comparable_volume",
        reference_price=ref_price,
    )
    top5_volume = float(
        bins.sort_values("comparable_volume", ascending=False)
        .head(5)["comparable_volume"]
        .sum()
    )
    below_amount = float(bins.loc[bins["price_bin"] < ref_price, "amount"].sum())
    above_amount = float(bins.loc[bins["price_bin"] > ref_price, "amount"].sum())
    below_volume = float(
        bins.loc[bins["price_bin"] < ref_price, "comparable_volume"].sum()
    )
    above_volume = float(
        bins.loc[bins["price_bin"] > ref_price, "comparable_volume"].sum()
    )

    return VolumeProfileCalculation(
        status="available",
        price_basis=price_basis,
        profile=VolumeProfile(
            ts_code=ts_code,
            reference_date=reference_date,
            lookback_days=lookback_days,
            start_date=dates[0],
            end_date=dates[-1],
            rows_count=len(payload),
            total_vol=total_vol,
            total_amount=total_amount,
            vwap=adjusted_vwap,
            poc_price=poc_price,
            value_area_low=value_low,
            value_area_high=value_high,
            concentration_top5_pct=top5_volume / total_vol * 100,
            below_reference_amount_pct=below_amount / total_amount * 100,
            above_reference_amount_pct=above_amount / total_amount * 100,
            below_reference_volume_pct=below_volume / total_vol * 100,
            above_reference_volume_pct=above_volume / total_vol * 100,
        ),
    )


def calculate_volume_profile(
    store: DuckDBStore,
    ts_code: str,
    *,
    reference_date: date,
    lookback_days: int,
    freq: str = "1min",
    bin_pct: float = 0.005,
    bin_ratio: float | None = None,
) -> VolumeProfile | None:
    """兼容接口：仅返回 profile，不把诊断字符串混入策略输入。"""
    return calculate_volume_profile_outcome(
        store,
        ts_code,
        reference_date=reference_date,
        lookback_days=lookback_days,
        freq=freq,
        bin_pct=bin_pct,
        bin_ratio=bin_ratio,
    ).profile


def scale_volume_profile(
    profile: VolumeProfile,
    ratio: float,
    *,
    target_reference_date: date | None = None,
) -> VolumeProfile:
    """把价量分布统一到新的价格与可比股数基准。"""
    if ratio <= 0:
        raise ValueError("ratio must be positive")

    updates: dict[str, object] = {}
    if target_reference_date is not None:
        updates["reference_date"] = target_reference_date
    if ratio != 1:
        updates.update(
            {
                "vwap": profile.vwap * ratio,
                "poc_price": profile.poc_price * ratio,
                "value_area_low": profile.value_area_low * ratio,
                "value_area_high": profile.value_area_high * ratio,
                "total_vol": profile.total_vol / ratio,
            }
        )
    if not updates:
        return profile
    return profile.model_copy(update=updates)


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
