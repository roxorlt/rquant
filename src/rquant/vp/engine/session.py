"""Session VP：盘中单日增量累加器（规格 §3.3.5 表格第一行）。

窗口是当日 09:30–15:00，更新方式是「盘中每分钟增量累加，收盘后定稿」，任意时刻
可取快照。重启恢复走 `SessionVP.from_minutes`——规格 §3.0 要求状态实时落库、崩溃
重启后可恢复，计算侧的配合就是这条重建路径：**重建即重放**，只有一条累加代码路径，
增量结果与重建结果因此逐比特一致，不存在「两份实现慢慢分叉」的空间。

（rQuant 已经在 surge-watch 上踩过一次：当日累计量只存内存，进程重启后归零，
2026-07-07 全天早盘信号丢失。）
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import datetime
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.vp.engine.config import VPEngineConfig
from rquant.vp.engine.profile import ProfileAccumulator, VPProfile


class MinuteBar(BaseModel):
    """分钟 K。`volume`/`amount` 的单位由调用方保证一致，引擎不做换算。

    规格 §2.1 已把单位列为数据字典必填列：`daily_bar.amount` 是千元、
    `rt_min.amount` 是元，差 1000 倍，混用会让 VWAP 直接错三个数量级。
    """

    model_config = ConfigDict(frozen=True)

    trade_time: datetime
    high: float = Field(gt=0)
    low: float = Field(gt=0)
    close: float = Field(gt=0)
    volume: float = Field(ge=0)
    amount: float = Field(default=0.0, ge=0)

    @model_validator(mode="after")
    def _validate_range(self) -> Self:
        if self.high < self.low:
            msg = f"最高价低于最低价：{self.trade_time} low={self.low} high={self.high}"
            raise ValueError(msg)
        if not (self.low <= self.close <= self.high):
            msg = f"收盘价不在 [low, high] 内：{self.trade_time} close={self.close}"
            raise ValueError(msg)
        return self


class SessionVP:
    """当日 Session VP 累加器。

    参考价（决定 bin 宽度）默认取首根分钟 K 的收盘价；传 `reference_price`
    可改用昨收等外部基准，一经确定不再变更（理由见 `ProfileAccumulator`）。
    """

    def __init__(
        self,
        config: VPEngineConfig,
        *,
        reference_price: float | None = None,
    ) -> None:
        self._accumulator = ProfileAccumulator(config, reference_price=reference_price)
        self._last_trade_time: datetime | None = None

    @classmethod
    def from_minutes(
        cls,
        minutes: Sequence[MinuteBar],
        *,
        config: VPEngineConfig,
        reference_price: float | None = None,
    ) -> Self:
        """从分钟列表重建（进程重启恢复）。分钟按 `trade_time` 升序重放。"""
        session = cls(config, reference_price=reference_price)
        session.update_many(sorted(minutes, key=lambda bar: bar.trade_time))
        return session

    @property
    def config(self) -> VPEngineConfig:
        return self._accumulator.config

    @property
    def bar_count(self) -> int:
        return self._accumulator.bar_count

    @property
    def total_volume(self) -> float:
        return self._accumulator.total_volume

    @property
    def bin_width(self) -> float | None:
        return self._accumulator.bin_width

    @property
    def reference_price(self) -> float | None:
        return self._accumulator.reference_price

    @property
    def last_trade_time(self) -> datetime | None:
        return self._last_trade_time

    def update(self, bar: MinuteBar) -> None:
        """吃一根分钟 K。乱序到达直接拒绝——VP 本身与顺序无关，但乱序意味着上游
        数据流出了问题，静默接受会让重建结果与增量结果的一致性无从验证。"""
        if self._last_trade_time is not None and bar.trade_time <= self._last_trade_time:
            msg = f"分钟乱序或重复：{bar.trade_time} 不晚于已累加的 {self._last_trade_time}"
            raise ValueError(msg)
        self._accumulator.add(
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume=bar.volume,
            amount=bar.amount,
        )
        self._last_trade_time = bar.trade_time

    def update_many(self, bars: Iterable[MinuteBar]) -> None:
        for bar in bars:
            self.update(bar)

    def snapshot(self) -> VPProfile | None:
        """任意时刻的 POC / VAH / VAL / bin 直方图；当日尚无成交时返回 None。"""
        return self._accumulator.snapshot()
