"""VP 引擎参数（规格 §10.1 参数总表，默认值逐条抄表）。

单位口径不统一是这张表最容易出事的地方，逐字段说明：

- `anchor_pct_min` 是**百分数**（7.0 = 7%），与 `daily_bar.pct_chg` 的落库口径一致，
  比较时不做换算；
- `avp_distance_max` / `bin_ratio` / `hvn_threshold` / `lvn_threshold` 是**比例**
  （0.15 = 15%），由本引擎自己算，不来自数据源。
"""

from __future__ import annotations

from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

VA_PCT_SPEC = 0.70
"""价值区覆盖比例，规格 §10.1 标注「固定」——行业标准，不参与标定。"""


class VPEngineConfig(BaseModel):
    """VP 计算参数。规格 §10.1 打 ⚠️ 的项一律从这里读，引擎内不写死魔数。"""

    model_config = ConfigDict(frozen=True)

    composite_window: int = Field(default=60, gt=0, description="复合 VP 窗口（交易日）")
    anchor_lookback: int = Field(default=15, gt=0, description="锚点搜索范围（交易日）")
    anchor_pct_min: float = Field(
        default=7.0,
        gt=0,
        description="锚点大阳线涨幅门槛，百分数口径（7.0 = 7%）",
    )
    anchor_vol_ratio: float = Field(default=2.0, gt=0, description="锚点量能倍数")
    anchor_avg_window: int = Field(
        default=20,
        gt=0,
        description="锚点量能基准窗口，规格 §3.3.5 的 avg20_amount",
    )
    avp_distance_max: float = Field(
        default=0.15,
        gt=0,
        lt=1,
        description="距锚点 POC 的最大偏离，比例口径（0.15 = 15%）",
    )
    bin_ratio: float = Field(
        default=0.002,
        gt=0,
        lt=0.1,
        description="bin 宽度 = max(0.01, round(ref_price × bin_ratio, 2))，规格 §3.3.1",
    )
    va_pct: float = Field(
        default=VA_PCT_SPEC,
        description="价值区覆盖比例，规格 §10.1 固定为 0.70",
    )
    hvn_threshold: float = Field(
        default=0.60,
        gt=0,
        le=1,
        description="HVN 判定：bin 成交量 ≥ 最大 bin × 该值",
    )
    lvn_threshold: float = Field(
        default=0.30,
        gt=0,
        le=1,
        description="LVN 判定：段内 bin ≤ 两侧最近 HVN 较小者 × 该值",
    )
    lvn_min_width: int = Field(default=3, ge=1, description="LVN 最小段宽（bin 数）")
    vol_alloc_algo: Literal["uniform"] = Field(
        default="uniform",
        description="分钟线→VP 分配算法，规格 §3.3.2 默认取算法 U，W 未做对照前不得启用",
    )

    @model_validator(mode="after")
    def _va_pct_is_fixed(self) -> Self:
        # 规格 §10.1 把 va_pct 列为「固定」：70% 是 Market Profile 的行业口径，
        # 允许它被调等于允许「调宽价值区让回测好看」，属于 §7 审计清单要抓的行为。
        if self.va_pct != VA_PCT_SPEC:
            msg = f"va_pct 由规格 §10.1 固定为 {VA_PCT_SPEC}，不得标定；收到 {self.va_pct}"
            raise ValueError(msg)
        return self
