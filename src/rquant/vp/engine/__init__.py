"""🔒 VP 回测底座：纯函数 + Pydantic，无 IO。

权限边界见 `src/rquant/vp/README.md` 与规格 §7：本目录对研究沙盒只读，改动必须
走人工 review，且改后要重跑全部历史实验并在 `experiment_log.csv` 标注「引擎版本变更」。

模块分工：

- `config`   参数总表（规格 §10.1），引擎内不写死魔数
- `profile`  分桶、算法 U 均摊、POC/价值区快照（§3.3.1–§3.3.3）
- `session`  盘中 Session VP 增量累加器（§3.3.5）
- `anchored` 锚点选取与 Anchored VP（§3.3.5）
- `lvn`      HVN / LVN 检测（§3.3.3）
"""

from __future__ import annotations

from rquant.vp.engine.anchored import (
    DailyBar,
    anchored_profile_from_daily,
    anchored_profile_from_minutes,
    resolve_anchor_date,
    select_anchor_date,
)
from rquant.vp.engine.config import VA_PCT_SPEC, VPEngineConfig
from rquant.vp.engine.lvn import (
    HighVolumeNode,
    LowVolumeNode,
    VolumeNodes,
    detect_hvn,
    detect_lvn,
    detect_nodes,
)
from rquant.vp.engine.profile import (
    ProfileAccumulator,
    VPBin,
    VPProfile,
    allocate_uniform,
    bin_index,
    bin_mid_price,
    build_profile,
    resolve_bin_width,
)
from rquant.vp.engine.session import MinuteBar, SessionVP

ENGINE_VERSION = "0.1.0"
"""引擎版本。`sandbox/experiment_log.csv` 的 `engine_version` 列写这个值——
规格 §7 要求引擎变更后重跑历史实验，没有版本号就无从判断哪些结果需要重跑。"""

__all__ = [
    "ENGINE_VERSION",
    "VA_PCT_SPEC",
    "DailyBar",
    "HighVolumeNode",
    "LowVolumeNode",
    "MinuteBar",
    "ProfileAccumulator",
    "SessionVP",
    "VPBin",
    "VPEngineConfig",
    "VPProfile",
    "VolumeNodes",
    "allocate_uniform",
    "anchored_profile_from_daily",
    "anchored_profile_from_minutes",
    "bin_index",
    "bin_mid_price",
    "build_profile",
    "detect_hvn",
    "detect_lvn",
    "detect_nodes",
    "resolve_anchor_date",
    "resolve_bin_width",
    "select_anchor_date",
]
