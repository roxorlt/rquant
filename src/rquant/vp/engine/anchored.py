"""Anchored VP：锚点选取 + 从锚点日起算的价量分布（规格 §3.3.5）。

锚点规则（规格把原策略的含糊表述定死了，逐条对应下面的实现）：

- 扫描近 `anchor_lookback` 个交易日，找出所有满足「涨幅 > `anchor_pct_min`
  且 `amount ≥ anchor_vol_ratio × avg20_amount`」的交易日；
- 若有多个，取**最近**的一个，不是涨幅最大的；
- 一个都没有 → 该票无锚点 → 不进 L1；
- 锚点一经选定，在该股退出 L1 之前不再变更。「退出 L1」是调用方的状态，引擎
  只提供 `resolve_anchor_date`：给了旧锚点就原样返回，锚点漂移会让 AVP_POC
  每天跳变，那是规格明令要防的。

Profile 侧提供两个入口，输入类型显式区分，不做运行时嗅探：
`anchored_profile_from_minutes`（分钟集，回测层精度）与
`anchored_profile_from_daily`（日线集，锚点回溯超出分钟覆盖范围时的降级路径）。
两者都走算法 U，输出同一个 `VPProfile`。
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.vp.engine.config import VPEngineConfig
from rquant.vp.engine.profile import ProfileAccumulator, VPProfile
from rquant.vp.engine.session import MinuteBar


class DailyBar(BaseModel):
    """日 K。`pct_chg` 用百分数口径（7.0 = 7%），与 `daily_bar.pct_chg` 落库一致。"""

    model_config = ConfigDict(frozen=True)

    trade_date: date
    high: float = Field(gt=0)
    low: float = Field(gt=0)
    close: float = Field(gt=0)
    volume: float = Field(ge=0)
    amount: float = Field(ge=0)
    pct_chg: float = 0.0

    @model_validator(mode="after")
    def _validate_range(self) -> Self:
        if self.high < self.low:
            msg = f"最高价低于最低价：{self.trade_date} low={self.low} high={self.high}"
            raise ValueError(msg)
        if not (self.low <= self.close <= self.high):
            msg = f"收盘价不在 [low, high] 内：{self.trade_date} close={self.close}"
            raise ValueError(msg)
        return self


def _sorted_daily(bars: Sequence[DailyBar]) -> list[DailyBar]:
    ordered = sorted(bars, key=lambda bar: bar.trade_date)
    dates = [bar.trade_date for bar in ordered]
    if len(set(dates)) != len(dates):
        msg = "日线序列存在重复交易日"
        raise ValueError(msg)
    return ordered


def select_anchor_date(
    bars: Sequence[DailyBar],
    *,
    as_of: date,
    config: VPEngineConfig,
) -> date | None:
    """在 `as_of`（含）之前的近 N 个交易日里选锚点日，取最近的合格日；无则 None。

    量能基准 `avg20_amount` 取候选日**之前** `anchor_avg_window` 个交易日的成交额均值：
    含当日会让「放量」和自己比，门槛自动放松，属于典型的未来函数式自欺。
    前置样本不足的候选日直接跳过，不猜。
    """
    ordered = _sorted_daily(bars)
    window = [bar for bar in ordered if bar.trade_date <= as_of][-config.anchor_lookback :]
    if not window:
        return None

    position = {bar.trade_date: index for index, bar in enumerate(ordered)}
    for candidate in reversed(window):
        if candidate.pct_chg <= config.anchor_pct_min:
            continue
        index = position[candidate.trade_date]
        if index < config.anchor_avg_window:
            continue
        prior = ordered[index - config.anchor_avg_window : index]
        avg_amount = sum(bar.amount for bar in prior) / len(prior)
        if avg_amount <= 0:
            continue
        if candidate.amount >= config.anchor_vol_ratio * avg_amount:
            return candidate.trade_date
    return None


def resolve_anchor_date(
    bars: Sequence[DailyBar],
    *,
    as_of: date,
    config: VPEngineConfig,
    current_anchor: date | None = None,
) -> date | None:
    """锚点一经选定不再变更；调用方在该股退出 L1 时传 `current_anchor=None` 重选。"""
    if current_anchor is not None:
        return current_anchor
    return select_anchor_date(bars, as_of=as_of, config=config)


def anchored_profile_from_minutes(
    minutes: Sequence[MinuteBar],
    *,
    anchor_date: date,
    config: VPEngineConfig,
    reference_price: float | None = None,
) -> VPProfile | None:
    """分钟集输入：取 `trade_time.date() >= anchor_date` 的全部分钟，只增不减。"""
    kept = sorted(
        (bar for bar in minutes if bar.trade_time.date() >= anchor_date),
        key=lambda bar: bar.trade_time,
    )
    accumulator = ProfileAccumulator(config, reference_price=reference_price)
    for bar in kept:
        accumulator.add(
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume=bar.volume,
            amount=bar.amount,
        )
    return accumulator.snapshot()


def anchored_profile_from_daily(
    bars: Sequence[DailyBar],
    *,
    anchor_date: date,
    config: VPEngineConfig,
    reference_price: float | None = None,
) -> VPProfile | None:
    """日线集输入：锚点早于分钟覆盖范围时的降级路径，形态更粗但可回溯多年。"""
    kept = [bar for bar in _sorted_daily(bars) if bar.trade_date >= anchor_date]
    accumulator = ProfileAccumulator(config, reference_price=reference_price)
    for bar in kept:
        accumulator.add(
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume=bar.volume,
            amount=bar.amount,
        )
    return accumulator.snapshot()
