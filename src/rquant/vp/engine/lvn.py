"""HVN / LVN 检测（规格 §3.3.3）。

- **HVN**：局部极大值 bin，且成交量 ≥ 窗口内最大 bin × `hvn_threshold`。
- **LVN**：同时满足三条的连续 bin 段——
  1. 段内每个 bin 的成交量 ≤ 相邻两侧最近 HVN 成交量的 `lvn_threshold`；
  2. 段宽 ≥ `lvn_min_width`；
  3. 段两侧各存在一个 HVN（真的是夹在两座山之间的谷）。

条件 1 的「两侧最近 HVN」按**较小的那座山**取阈值：规格写的是「≤ 相邻两侧最近 HVN
成交量的 30%」，两侧都要满足，等价于 ≤ min(左, 右) × 30%。取 max 会让一座大山旁边
的浅坑也算成谷，那不是 LVN 想描述的形态。

条件 3 由算法结构保证：只在**相邻两个 HVN 之间**找谷，段外侧天然各有一座山。因此
剖面边缘的凹陷（单侧无山）不会被检出——这是规格要的，不是遗漏。

三个阈值全部走 `VPEngineConfig`，规格 §10.1 标注它们均待 P1 标定，代码里不写死。
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from rquant.vp.engine.config import VPEngineConfig
from rquant.vp.engine.profile import VPProfile


class HighVolumeNode(BaseModel):
    """高成交节点（HVN）。`position` 是在 `VPProfile.bins` 中的下标。"""

    model_config = ConfigDict(frozen=True)

    position: int = Field(ge=0)
    price: float
    volume: float = Field(gt=0)


class LowVolumeNode(BaseModel):
    """低成交凹陷区（LVN），策略里的第一止盈点。"""

    model_config = ConfigDict(frozen=True)

    start_position: int = Field(ge=0)
    end_position: int = Field(ge=0)
    width: int = Field(ge=1)
    low_price: float = Field(description="段内最低 bin 的中值价")
    high_price: float = Field(description="段内最高 bin 的中值价")
    low_edge: float
    high_edge: float
    trough_price: float = Field(description="段内成交量最小 bin 的中值价，即谷底")
    min_volume: float = Field(ge=0)
    max_volume: float = Field(ge=0)
    volume_limit: float = Field(gt=0, description="判定阈值 = min(左 HVN, 右 HVN) × lvn_threshold")
    left_hvn: HighVolumeNode
    right_hvn: HighVolumeNode


class VolumeNodes(BaseModel):
    """一次检测的完整结果。"""

    model_config = ConfigDict(frozen=True)

    hvns: tuple[HighVolumeNode, ...]
    lvns: tuple[LowVolumeNode, ...]


def detect_hvn(profile: VPProfile, *, config: VPEngineConfig) -> tuple[HighVolumeNode, ...]:
    """局部极大且 ≥ 最大 bin × hvn_threshold。

    平台型（相邻等量）用 `>=` 判局部极大，会同时收下平台上的多个 bin；这是有意的，
    改成严格大于会让完全等高的平台一个都选不出来。
    """
    volumes = profile.volumes
    prices = profile.prices
    peak = max(volumes)
    if peak <= 0:
        return ()
    floor = peak * config.hvn_threshold
    last = len(volumes) - 1
    nodes: list[HighVolumeNode] = []
    for position, volume in enumerate(volumes):
        if volume < floor:
            continue
        left_ok = position == 0 or volume >= volumes[position - 1]
        right_ok = position == last or volume >= volumes[position + 1]
        if left_ok and right_ok:
            nodes.append(
                HighVolumeNode(position=position, price=prices[position], volume=volume)
            )
    return tuple(nodes)


def _qualifying_runs(
    volumes: tuple[float, ...],
    *,
    start: int,
    end: int,
    limit: float,
) -> list[tuple[int, int]]:
    """[start, end] 闭区间内，成交量 ≤ limit 的极大连续段。"""
    runs: list[tuple[int, int]] = []
    run_start: int | None = None
    for position in range(start, end + 1):
        if volumes[position] <= limit:
            if run_start is None:
                run_start = position
            continue
        if run_start is not None:
            runs.append((run_start, position - 1))
            run_start = None
    if run_start is not None:
        runs.append((run_start, end))
    return runs


def detect_lvn(profile: VPProfile, *, config: VPEngineConfig) -> tuple[LowVolumeNode, ...]:
    """在相邻 HVN 之间找满足三条件的谷。HVN 少于两个时必然无 LVN。"""
    hvns = detect_hvn(profile, config=config)
    volumes = profile.volumes
    nodes: list[LowVolumeNode] = []
    for left, right in zip(hvns, hvns[1:], strict=False):
        gap = right.position - left.position - 1
        if gap < config.lvn_min_width:
            continue
        limit = min(left.volume, right.volume) * config.lvn_threshold
        if limit <= 0:
            continue
        for run_start, run_end in _qualifying_runs(
            volumes,
            start=left.position + 1,
            end=right.position - 1,
            limit=limit,
        ):
            width = run_end - run_start + 1
            if width < config.lvn_min_width:
                continue
            segment = volumes[run_start : run_end + 1]
            trough = run_start + segment.index(min(segment))
            nodes.append(
                LowVolumeNode(
                    start_position=run_start,
                    end_position=run_end,
                    width=width,
                    low_price=profile.bins[run_start].price,
                    high_price=profile.bins[run_end].price,
                    low_edge=profile.bins[run_start].low_edge,
                    high_edge=profile.bins[run_end].high_edge,
                    trough_price=profile.bins[trough].price,
                    min_volume=min(segment),
                    max_volume=max(segment),
                    volume_limit=limit,
                    left_hvn=left,
                    right_hvn=right,
                )
            )
    return tuple(nodes)


def detect_nodes(profile: VPProfile, *, config: VPEngineConfig) -> VolumeNodes:
    """一次性拿到 HVN 与 LVN。"""
    return VolumeNodes(
        hvns=detect_hvn(profile, config=config),
        lvns=detect_lvn(profile, config=config),
    )
