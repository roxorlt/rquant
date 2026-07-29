"""VP 直方图基元：分桶、算法 U 均摊、POC/价值区快照（规格 §3.3.1–§3.3.3）。

三条硬约束，改动前先读规格：

1. **价值区不自己算**。POC 并列裁决与价值区连续扩展一律复用
   `rquant.volume_profile` 里 PR #140（G20）修好的那份实现，全仓只此一份。
2. **分桶不写死**。宽度走 `VPEngineConfig.bin_ratio`，落桶用 `Decimal` 而不是浮点
   除法——`floor(price / width)` 在 0.02 这类非精确二进制宽度上会掉到相邻桶。
3. **直方图是稠密的**。从最低到最高 bin 逐格铺开、无成交处补 0，否则 §3.3.3 的
   「连续 bin 段」定义（LVN 段宽）没有意义。
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from decimal import ROUND_FLOOR, Decimal
from typing import Self

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.volume_profile import _poc_index, _resolve_bin_size, _value_area
from rquant.vp.engine.config import VPEngineConfig


class VPBin(BaseModel):
    """一个价格桶。`price` 是规格 §3.3.3 所说的 bin 中值价。"""

    model_config = ConfigDict(frozen=True)

    index: int
    price: float
    low_edge: float
    high_edge: float
    volume: float = Field(ge=0)


class VPProfile(BaseModel):
    """任意窗口的价量分布快照（Session / Anchored / Composite 共用一种输出）。"""

    model_config = ConfigDict(frozen=True)

    bin_width: float = Field(gt=0)
    reference_price: float = Field(gt=0)
    va_pct: float = Field(gt=0, le=1)
    bins: tuple[VPBin, ...]
    total_volume: float = Field(gt=0)
    total_amount: float = Field(ge=0)
    bar_count: int = Field(ge=1)
    price_low: float
    price_high: float
    poc_position: int = Field(ge=0, description="POC 在 bins 元组中的下标，不是 bin index")
    poc_price: float
    value_area_low: float
    value_area_high: float
    vol_alloc_algo: str

    @model_validator(mode="after")
    def _validate_shape(self) -> Self:
        if not self.bins:
            msg = "VPProfile 至少需要一个 bin"
            raise ValueError(msg)
        indexes = [item.index for item in self.bins]
        if indexes != list(range(indexes[0], indexes[0] + len(indexes))):
            msg = "VPProfile.bins 必须是稠密连续的 bin 序列"
            raise ValueError(msg)
        if self.poc_position >= len(self.bins):
            msg = "poc_position 越界"
            raise ValueError(msg)
        if self.value_area_low > self.value_area_high:
            msg = "value_area_low 不得高于 value_area_high"
            raise ValueError(msg)
        if not (self.value_area_low <= self.poc_price <= self.value_area_high):
            msg = "POC 必须落在价值区内"
            raise ValueError(msg)
        return self

    @property
    def prices(self) -> tuple[float, ...]:
        return tuple(item.price for item in self.bins)

    @property
    def volumes(self) -> tuple[float, ...]:
        return tuple(item.volume for item in self.bins)

    @property
    def value_area_width(self) -> float:
        return self.value_area_high - self.value_area_low


def resolve_bin_width(reference_price: float, *, bin_ratio: float) -> float:
    """bin 宽度 = max(0.01, round(ref_price × bin_ratio, 2))（规格 §3.3.1）。

    直接复用生产入口 `_resolve_bin_size` 的 `bin_ratio` 分支（PR #140 加的），
    避免同一个公式在仓库里出现第二份。
    """
    if reference_price <= 0:
        msg = f"参考价必须为正：{reference_price}"
        raise ValueError(msg)
    return _resolve_bin_size(reference_price, bin_pct=bin_ratio, bin_ratio=bin_ratio)


def bin_index(price: float, bin_width: float) -> int:
    """价格落桶。用 Decimal 取整，浮点除法在非精确宽度上会掉桶。"""
    quotient = Decimal(str(price)) / Decimal(str(bin_width))
    return int(quotient.to_integral_value(rounding=ROUND_FLOOR))


def bin_low_edge(index: int, bin_width: float) -> float:
    return float(Decimal(index) * Decimal(str(bin_width)))


def bin_mid_price(index: int, bin_width: float) -> float:
    width = Decimal(str(bin_width))
    return float(Decimal(index) * width + width / 2)


def allocate_uniform(
    *,
    low: float,
    high: float,
    volume: float,
    bin_width: float,
) -> dict[int, float]:
    """算法 U（规格 §3.3.2）：该根 K 的成交量在 [low, high] 覆盖的所有 bin 上均摊。

    规格默认取 U 而不是三角权重 W：无参数、可复现；W 引入未经验证的形态假设，
    P1 对照实验做完之前不得切换。
    """
    if volume <= 0:
        return {}
    first = bin_index(low, bin_width)
    last = bin_index(high, bin_width)
    if last < first:
        msg = f"K 线最高价低于最低价：low={low} high={high}"
        raise ValueError(msg)
    span = last - first + 1
    share = volume / span
    return {first + offset: share for offset in range(span)}


def build_profile(
    counts: Mapping[int, float],
    *,
    bin_width: float,
    reference_price: float,
    total_amount: float,
    bar_count: int,
    price_low: float,
    price_high: float,
    config: VPEngineConfig,
) -> VPProfile | None:
    """把稀疏的 {bin_index: volume} 铺成稠密直方图并求 POC / 价值区。"""
    positive = {index: volume for index, volume in counts.items() if volume > 0}
    if not positive:
        return None

    first, last = min(positive), max(positive)
    bins = tuple(
        VPBin(
            index=index,
            price=bin_mid_price(index, bin_width),
            low_edge=bin_low_edge(index, bin_width),
            high_edge=bin_low_edge(index + 1, bin_width),
            volume=positive.get(index, 0.0),
        )
        for index in range(first, last + 1)
    )
    prices = [item.price for item in bins]
    volumes = [item.volume for item in bins]

    # POC 与价值区都走 volume_profile 那份实现：并列裁决口径必须与生产完全一致
    poc_position = _poc_index(prices, volumes, reference_price)
    frame = pd.DataFrame({"price_bin": prices, "volume": volumes})
    value_low, value_high = _value_area(
        frame,
        weight_column="volume",
        ratio=config.va_pct,
        reference_price=reference_price,
    )
    return VPProfile(
        bin_width=bin_width,
        reference_price=reference_price,
        va_pct=config.va_pct,
        bins=bins,
        total_volume=sum(volumes),
        total_amount=total_amount,
        bar_count=bar_count,
        price_low=price_low,
        price_high=price_high,
        poc_position=poc_position,
        poc_price=prices[poc_position],
        value_area_low=value_low,
        value_area_high=value_high,
        vol_alloc_algo=config.vol_alloc_algo,
    )


class ProfileAccumulator:
    """按 K 线增量累加的直方图。Session VP 与 Anchored VP 共用同一条计算路径。

    参考价一经确定不再变更：规格 §3.3.1 允许「窗口均价」或「首日收盘价」二选一，
    增量累加只能选后者——窗口均价随数据到达而变，bin 宽度跟着变，已累加的桶就全废了。
    """

    def __init__(
        self,
        config: VPEngineConfig,
        *,
        reference_price: float | None = None,
    ) -> None:
        self._config = config
        self._reference_price = reference_price
        self._bin_width: float | None = None
        if reference_price is not None:
            self._bin_width = resolve_bin_width(reference_price, bin_ratio=config.bin_ratio)
        self._counts: dict[int, float] = {}
        self._total_amount = 0.0
        self._bar_count = 0
        self._price_low: float | None = None
        self._price_high: float | None = None

    @property
    def config(self) -> VPEngineConfig:
        return self._config

    @property
    def bin_width(self) -> float | None:
        return self._bin_width

    @property
    def reference_price(self) -> float | None:
        return self._reference_price

    @property
    def bar_count(self) -> int:
        return self._bar_count

    @property
    def total_volume(self) -> float:
        return sum(self._counts.values())

    def add(
        self,
        *,
        high: float,
        low: float,
        close: float,
        volume: float,
        amount: float = 0.0,
    ) -> None:
        if high < low:
            msg = f"K 线最高价低于最低价：low={low} high={high}"
            raise ValueError(msg)
        if volume < 0:
            msg = f"成交量不得为负：{volume}"
            raise ValueError(msg)
        bin_width = self._ensure_bin_width(close)
        self._bar_count += 1
        self._price_low = low if self._price_low is None else min(self._price_low, low)
        self._price_high = high if self._price_high is None else max(self._price_high, high)
        if volume == 0:
            return
        self._total_amount += amount
        for index, share in allocate_uniform(
            low=low,
            high=high,
            volume=volume,
            bin_width=bin_width,
        ).items():
            self._counts[index] = self._counts.get(index, 0.0) + share

    def _ensure_bin_width(self, close: float) -> float:
        """首根 K 线定下参考价与 bin 宽度，此后不再变更。"""
        if self._bin_width is not None:
            return self._bin_width
        if close <= 0:
            msg = f"首根 K 线收盘价非法，无法确定参考价：{close}"
            raise ValueError(msg)
        self._reference_price = close
        self._bin_width = resolve_bin_width(close, bin_ratio=self._config.bin_ratio)
        return self._bin_width

    def snapshot(self) -> VPProfile | None:
        """任意时刻可取；尚无成交量时返回 None。"""
        if self._bin_width is None or self._reference_price is None:
            return None
        if self._price_low is None or self._price_high is None:
            return None
        return build_profile(
            self._counts,
            bin_width=self._bin_width,
            reference_price=self._reference_price,
            total_amount=self._total_amount,
            bar_count=self._bar_count,
            price_low=self._price_low,
            price_high=self._price_high,
            config=self._config,
        )


def accumulate_bars(
    bars: Iterable[tuple[float, float, float, float, float]],
    *,
    config: VPEngineConfig,
    reference_price: float | None = None,
) -> ProfileAccumulator:
    """把 (high, low, close, volume, amount) 序列灌进累加器，供函数式入口复用。"""
    accumulator = ProfileAccumulator(config, reference_price=reference_price)
    for high, low, close, volume, amount in bars:
        accumulator.add(high=high, low=low, close=close, volume=volume, amount=amount)
    return accumulator
