"""Composite VP：最近 60 个交易日的复合价量分布（规格 §3.3.5 第二行、§8.2、§2.3）。

## 为什么必须增量

规格 §8.2 写死：「复合 VP 必须**增量滚动**（每日加一天、减一天）。全量是
5400 × 60 × 240 ≈ 7.8 亿行，每天重算不现实。」所以本实现把**单日的桶直方图**
当作滚动窗口的最小单元：加一天是直方图相加，挤出第 61 天是直方图相减，任何时候
都不回头重扫窗口内的分钟线。落库侧只需要存 `DayHistogram`（一天几百个
`(bin_index, volume)` 二元组），比存 60 天分钟线小两个数量级。

## bin 对齐的设计裁决（本模块最容易出错的地方）

不同日的单日 profile 各自锚在当日参考价上，bin 宽度不同 → 网格错位 → 直方图**不能**
直接相加。两个候选方案：

- **方案 A（本实现）**：`CompositeVP` 构造时固定自己的网格（锚定窗口首日参考价），
  `push_day` 收**该日的原始分钟集**，用本窗口的网格现场分桶。
- 方案 B：`push_day` 收已经算好的单日 `VPProfile`，把每个 bin 的量按其中值价重新
  映射到本窗口的网格上。

**选 A。**理由与代价：

1. 方案 B 是二次量化：整桶的量被塞进「中值价所落的那个新桶」，单日误差最大半个 bin，
   而这个误差在 60 天窗口里同向累积。POC 只要偏 1 个 bin，§3.4 的买点判定就可能翻转，
   这是策略级的错误，不是数值噪声。
2. 方案 A 与「用同样 60 天数据一次性重建」走**完全同一条**累加路径
   （`ProfileAccumulator` + 算法 U），逐 bar 算术一致，于是「滚动结果 == 重建结果」
   成为可断言的恒等式（见 `tests/unit/test_vp_composite.py` 的金标准用例）；
   方案 B 永远只能做到近似，一致性无从验证，也就无从发现回归。
3. 代价：调用方推日时必须给出**分钟集**，不能把当日 Session VP 的 profile 直接丢进来
   ——`SessionVP` 的网格锚在当日首根收盘价，与本窗口的网格通常不同宽。若调用方自己
   已经在本网格上分好桶（例如从库里读回历史 `DayHistogram`），走 `push_histogram`，
   宽度不符会被直接拒绝。

## 网格「扩展」为什么是零成本的

`bin_index(price) = floor(price / bin_width)`，锚点是**价格 0**，不是某个起始价——
网格是一把从 0 开始的无限刻度尺，不是一个有界数组。价格漂 30% 只是让新成交落到更高
（或更低）的 index 上，已入窗的 bin 的 index 与量值一个字节都不用动，稠密化在
`build_profile` 里按当前 min/max 现算。所以「网格按需向两端扩展」在本实现里不需要
任何代码，也不存在重分桶。

唯一会让网格失效的是 `bin_width` 变化，而它只在两处确定：窗口首日锚定，以及
`reset()` 之后由下一个入窗日重新锚定。

## 除权重置（规格 §2.3）

规格：「个股在窗口内发生除权除息 → 该个股的所有跨日 VP 从除权日重新起算，除权日
之前的成交量分布作废。」引擎**不自己判断除权**——那需要 `adj_factor`，属调用方职责；
这里只负责 `reset(reason)` 把窗口清干净并在状态里留痕（`reset_at` / `reset_reason`）。
重置会连参考价一起清空：10 送 10 之后价格腰斩，沿用除权前的 bin 宽度等于把分辨率砍半。
"""

from __future__ import annotations

from collections import deque
from collections.abc import Iterable, Sequence
from datetime import date
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.vp.engine.config import VPEngineConfig
from rquant.vp.engine.profile import (
    ProfileAccumulator,
    VPProfile,
    build_profile,
    resolve_bin_width,
)
from rquant.vp.engine.session import MinuteBar

COMPOSITE_STATE_VERSION = 1
"""`CompositeVPState` 的结构版本。落库后改字段必须同步 +1，`from_state` 会拒收旧版本。"""


class DayHistogram(BaseModel):
    """单个交易日在 **Composite 网格**上的桶计数：滚动窗口与落库的最小单元。

    `bin_width` 冗余存一份不是为了好看：从库里读回来的直方图必须能自证「是在哪把
    刻度尺上分的桶」，否则两张不同刻度的直方图叠加不会报错，只会静默算错。
    """

    model_config = ConfigDict(frozen=True)

    trade_date: date
    bin_width: float = Field(gt=0)
    counts: dict[int, float] = Field(default_factory=dict, description="{bin_index: volume}")
    total_amount: float = Field(default=0.0, ge=0)
    bar_count: int = Field(ge=1)
    price_low: float = Field(gt=0)
    price_high: float = Field(gt=0)

    @model_validator(mode="after")
    def _validate_shape(self) -> Self:
        if self.price_high < self.price_low:
            msg = f"{self.trade_date} 最高价低于最低价：{self.price_low} > {self.price_high}"
            raise ValueError(msg)
        if any(volume < 0 for volume in self.counts.values()):
            msg = f"{self.trade_date} 桶成交量不得为负"
            raise ValueError(msg)
        return self

    @property
    def total_volume(self) -> float:
        return sum(self.counts.values())


class CompositeSnapshot(BaseModel):
    """复合 VP 快照。

    窗口未满 60 天照常出快照（建仓初期、次新股、除权重置后都会遇到），由
    `days_count` 说明它是由几个交易日堆出来的——调用方要不要因为窗口太短而不下单，
    是策略层的判断，引擎不替它决定。
    """

    model_config = ConfigDict(frozen=True)

    profile: VPProfile
    window: int = Field(gt=0, description="规格要求的窗口长度，来自 config.composite_window")
    days_count: int = Field(gt=0, description="当前实际入窗的交易日数，可能小于 window")
    first_date: date
    last_date: date
    reset_at: date | None = Field(default=None, description="最近一次除权重置的交易日")
    reset_reason: str | None = None

    @property
    def is_full_window(self) -> bool:
        return self.days_count >= self.window

    @property
    def poc_price(self) -> float:
        return self.profile.poc_price

    @property
    def value_area_low(self) -> float:
        return self.profile.value_area_low

    @property
    def value_area_high(self) -> float:
        return self.profile.value_area_high


class CompositeVPState(BaseModel):
    """`CompositeVP` 的可序列化状态，为后续 `rvp.duckdb` 持久化留的接口。

    本模块不碰任何 IO：落库路径是调用方拿 `model_dump(mode="json")` 写库、
    读回来 `model_validate` 还原（dict 的 int 键在 JSON 里会变成字符串，
    Pydantic 会按声明的类型还原回 int）。

    运行态的滚动累计量（窗口合计直方图、引用计数、合计成交额）**不进状态**：
    它们全部由 `days` 派生，存两份就会有两份慢慢对不上。
    """

    model_config = ConfigDict(frozen=True)

    state_version: int = COMPOSITE_STATE_VERSION
    config: VPEngineConfig
    reference_price: float | None = None
    bin_width: float | None = None
    days: tuple[DayHistogram, ...] = ()
    reset_at: date | None = None
    reset_reason: str | None = None

    @model_validator(mode="after")
    def _validate_shape(self) -> Self:
        if len(self.days) > self.config.composite_window:
            msg = f"入窗天数 {len(self.days)} 超过窗口 {self.config.composite_window}"
            raise ValueError(msg)
        dates = [day.trade_date for day in self.days]
        if dates != sorted(set(dates)):
            msg = "days 必须按交易日严格升序且不重复"
            raise ValueError(msg)
        if self.days and self.bin_width is None:
            msg = "已有入窗数据却没有 bin_width，网格无从还原"
            raise ValueError(msg)
        if any(day.bin_width != self.bin_width for day in self.days):
            msg = "days 中存在与窗口网格宽度不一致的直方图"
            raise ValueError(msg)
        return self


class CompositeVP:
    """单只股票的 60 交易日滚动复合 VP。

    用法：每个交易日收盘后 `push_day(trade_date, minutes)` 推一天，窗口超长自动挤出
    最旧的一天；检测到除权时调用方先 `reset(reason)` 再继续推。任意时刻
    `snapshot()` 取 POC / VAH / VAL（价值区复用 `volume_profile` 那份实现，全仓只此一份）。

    参考价与 bin 宽度由**窗口首日**锚定，此后不变（除非 `reset`）。这不是偷懒：
    窗口均价随每天滚动而变，bin 宽度跟着变，已累积的桶就全废了，而且历史 POC 会随
    今天的数据被改写——那正是规格 §2.3 判死刑的「未来函数」形态。
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
        self._days: deque[DayHistogram] = deque()
        self._counts: dict[int, float] = {}
        # 每个 bin 被几个「在窗天」贡献过。挤出旧天时靠它决定整桶删除，见 `_evict`。
        self._refcounts: dict[int, int] = {}
        self._total_amount = 0.0
        self._bar_count = 0
        self._reset_at: date | None = None
        self._reset_reason: str | None = None

    @classmethod
    def from_days(
        cls,
        days: Iterable[tuple[date, Sequence[MinuteBar]]],
        *,
        config: VPEngineConfig,
        reference_price: float | None = None,
    ) -> Self:
        """从 (交易日, 分钟集) 序列重放。重建即重放——只有一条累加代码路径。"""
        composite = cls(config, reference_price=reference_price)
        for trade_date, minutes in sorted(days, key=lambda item: item[0]):
            composite.push_day(trade_date, minutes)
        return composite

    # ── 只读视图 ──────────────────────────────────────────────────────

    @property
    def config(self) -> VPEngineConfig:
        return self._config

    @property
    def window(self) -> int:
        return self._config.composite_window

    @property
    def days_count(self) -> int:
        return len(self._days)

    @property
    def day_histograms(self) -> tuple[DayHistogram, ...]:
        return tuple(self._days)

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

    @property
    def first_date(self) -> date | None:
        return self._days[0].trade_date if self._days else None

    @property
    def last_date(self) -> date | None:
        return self._days[-1].trade_date if self._days else None

    @property
    def reset_at(self) -> date | None:
        return self._reset_at

    @property
    def reset_reason(self) -> str | None:
        return self._reset_reason

    # ── 推日 ──────────────────────────────────────────────────────────

    def push_day(self, trade_date: date, minutes: Sequence[MinuteBar]) -> DayHistogram:
        """加一天，窗口超长时自动挤出最旧的一天，返回该日在本网格上的直方图。

        `minutes` 是该交易日的分钟集，分桶用**本窗口的网格**而不是当日 Session VP
        的网格（理由见模块 docstring 的设计裁决）。窗口首日会顺带锚定网格。
        """
        if not minutes:
            msg = f"{trade_date} 没有分钟数据，无法作为窗口内的一天"
            raise ValueError(msg)
        self._reject_out_of_order(trade_date)

        ordered = sorted(minutes, key=lambda bar: bar.trade_time)
        times = [bar.trade_time for bar in ordered]
        if len(set(times)) != len(times):
            msg = f"{trade_date} 分钟序列存在重复时间戳"
            raise ValueError(msg)
        foreign = sorted({item.date() for item in times} - {trade_date})
        if foreign:
            msg = f"分钟集混入了其他交易日：{foreign}（声明的是 {trade_date}）"
            raise ValueError(msg)

        accumulator = ProfileAccumulator(self._config, reference_price=self._reference_price)
        for bar in ordered:
            accumulator.add(
                high=bar.high,
                low=bar.low,
                close=bar.close,
                volume=bar.volume,
                amount=bar.amount,
            )
        if self._bin_width is None:
            # 窗口首日锚定网格；此后不再变更
            self._reference_price = accumulator.reference_price
            self._bin_width = accumulator.bin_width

        histogram = self._to_histogram(trade_date, accumulator)
        self._append(histogram)
        return histogram

    def push_histogram(self, histogram: DayHistogram) -> None:
        """加一天**已按本网格分桶**的直方图：从库里读回历史、或调用方自己预分桶时走这条。

        要求网格已经锚定（构造时给 `reference_price`，或先用 `push_day` 推过一天）。
        宽度不符直接拒绝：静默接受等于把两把不同刻度的尺子量出来的直方图叠在一起。
        """
        if self._bin_width is None:
            msg = (
                "网格尚未锚定：push_histogram 要求构造时给定 reference_price，"
                "或先用 push_day 由首日分钟集锚定网格"
            )
            raise ValueError(msg)
        if histogram.bin_width != self._bin_width:
            msg = f"直方图网格宽度 {histogram.bin_width} 与本窗口的 {self._bin_width} 不一致"
            raise ValueError(msg)
        self._reject_out_of_order(histogram.trade_date)
        self._append(histogram)

    def reset(
        self,
        reason: str,
        *,
        as_of: date | None = None,
        reference_price: float | None = None,
    ) -> None:
        """除权除息重置（规格 §2.3）：窗口内已有分布全部作废，从除权日重新起算。

        引擎不判断除权——那要 `adj_factor`，属调用方职责。这里只负责清干净并留痕。
        参考价一并清空，网格由重置后的第一个入窗日重新锚定（给了 `reference_price`
        就用它）：10 送 10 之后价格腰斩，沿用除权前的 bin 宽度等于把分辨率砍半。
        `as_of` 缺省取最后一个入窗交易日。
        """
        if not reason.strip():
            msg = "reset 必须给出原因：除权重置要在状态里留痕（规格 §2.3）"
            raise ValueError(msg)
        self._reset_at = as_of if as_of is not None else self.last_date
        self._reset_reason = reason
        self._days.clear()
        self._counts.clear()
        self._refcounts.clear()
        self._total_amount = 0.0
        self._bar_count = 0
        self._reference_price = reference_price
        self._bin_width = (
            resolve_bin_width(reference_price, bin_ratio=self._config.bin_ratio)
            if reference_price is not None
            else None
        )

    # ── 快照 ──────────────────────────────────────────────────────────

    def snapshot(self) -> CompositeSnapshot | None:
        """当前窗口的 POC / VAH / VAL 与稠密直方图；窗口空或全窗零成交时返回 None。"""
        if not self._days or self._bin_width is None or self._reference_price is None:
            return None
        profile = build_profile(
            self._counts,
            bin_width=self._bin_width,
            reference_price=self._reference_price,
            total_amount=self._total_amount,
            bar_count=self._bar_count,
            # min/max 不可由减法维护（减掉最低价那天，新的最低价是多少无从得知），
            # 每次快照在 ≤60 个日直方图上现算，代价可忽略
            price_low=min(day.price_low for day in self._days),
            price_high=max(day.price_high for day in self._days),
            config=self._config,
        )
        if profile is None:
            return None
        return CompositeSnapshot(
            profile=profile,
            window=self.window,
            days_count=len(self._days),
            first_date=self._days[0].trade_date,
            last_date=self._days[-1].trade_date,
            reset_at=self._reset_at,
            reset_reason=self._reset_reason,
        )

    # ── 序列化 ────────────────────────────────────────────────────────

    def to_state(self) -> CompositeVPState:
        """导出可落库状态。滚动累计量不导出，由 `days` 在 `from_state` 里重建。"""
        return CompositeVPState(
            state_version=COMPOSITE_STATE_VERSION,
            config=self._config,
            reference_price=self._reference_price,
            bin_width=self._bin_width,
            days=tuple(self._days),
            reset_at=self._reset_at,
            reset_reason=self._reset_reason,
        )

    @classmethod
    def from_state(cls, state: CompositeVPState) -> Self:
        """从落库状态还原。窗口合计直方图按 `days` 顺序重放，不另存一份。"""
        if state.state_version != COMPOSITE_STATE_VERSION:
            msg = (
                f"状态版本 {state.state_version} 与本引擎的 "
                f"{COMPOSITE_STATE_VERSION} 不符，需要迁移后再加载"
            )
            raise ValueError(msg)
        composite = cls(state.config)
        composite._reference_price = state.reference_price
        composite._bin_width = state.bin_width
        composite._reset_at = state.reset_at
        composite._reset_reason = state.reset_reason
        for histogram in state.days:
            composite._append(histogram)
        return composite

    # ── 内部 ──────────────────────────────────────────────────────────

    def _reject_out_of_order(self, trade_date: date) -> None:
        if self._days and trade_date <= self._days[-1].trade_date:
            msg = f"交易日乱序或重复：{trade_date} 不晚于已入窗的 {self._days[-1].trade_date}"
            raise ValueError(msg)

    def _to_histogram(self, trade_date: date, accumulator: ProfileAccumulator) -> DayHistogram:
        bin_width = accumulator.bin_width
        price_low = accumulator.price_low
        price_high = accumulator.price_high
        if bin_width is None or price_low is None or price_high is None:
            msg = f"{trade_date} 未能确定网格或价格区间，分钟集不合法"
            raise ValueError(msg)
        return DayHistogram(
            trade_date=trade_date,
            bin_width=bin_width,
            counts=dict(accumulator.counts),
            total_amount=accumulator.total_amount,
            bar_count=accumulator.bar_count,
            price_low=price_low,
            price_high=price_high,
        )

    def _append(self, histogram: DayHistogram) -> None:
        self._days.append(histogram)
        for index, volume in histogram.counts.items():
            if volume <= 0:
                continue
            self._counts[index] = self._counts.get(index, 0.0) + volume
            self._refcounts[index] = self._refcounts.get(index, 0) + 1
        self._total_amount += histogram.total_amount
        self._bar_count += histogram.bar_count
        while len(self._days) > self._config.composite_window:
            self._evict()

    def _evict(self) -> None:
        histogram = self._days.popleft()
        for index, volume in histogram.counts.items():
            if volume <= 0:
                continue
            remaining = self._refcounts[index] - 1
            if remaining <= 0:
                # 引用计数归零 → 整桶删除。若改成「减完约等于 0 就当没有」，浮点残渣
                # （量级 1e-11）会留在字典里，而 build_profile 只过滤 `volume > 0`，
                # 残渣会凭空撑出一个 bin，让滚动结果与重建结果的 bin 集合对不上。
                self._refcounts.pop(index, None)
                self._counts.pop(index, None)
                continue
            self._refcounts[index] = remaining
            self._counts[index] = max(0.0, self._counts[index] - volume)
        self._total_amount = max(0.0, self._total_amount - histogram.total_amount)
        self._bar_count -= histogram.bar_count
