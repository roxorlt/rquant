"""脉搏异动检测（挂 surge-watch 主循环）：滑窗规则 + jsonl 落盘，纯内存核心可单测。

时间轴用 surge_watch 的 241 交易分钟网格（grid_index），午休相邻——13:01 相对
11:30 只隔 1 个交易分钟，「10 分钟前」永远指 10 个交易分钟。本模块顶层 import
surge_watch / panorama_data；surge_watch 反向只在 run_surge_watch 内函数级
import 本模块，避免环。
"""

from __future__ import annotations

import json
from datetime import date, datetime
from datetime import time as dt_time
from pathlib import Path

from loguru import logger
from pydantic import BaseModel

from rquant.surge_watch import grid_index

PULSE_FILE_PREFIX = "pulse-"
ALERTS_FILE_PREFIX = "pulse_alerts-"


class PulsePoint(BaseModel):
    """一分钟的市场脉搏计数（jsonl 每行）。"""

    t: str  # HH:MM
    limit_up: int = 0
    limit_down: int = 0
    broken: int = 0
    up: int = 0
    down: int = 0
    up_ratio_pct: float | None = None
    total: int = 0


class PulseAlert(BaseModel):
    """一次脉搏异动事件（jsonl 每行 + 推送素材）。"""

    t: str
    kind: str        # limit_up_surge | broken_surge | limit_down_surge | ratio_jump
    kind_label: str  # 涨停潮 | 炸板潮 | 跌停潮 | 涨跌占比突变
    before: float
    after: float
    window_minutes: int
    message: str


class PulseConfig(BaseModel):
    """异动判定参数（对比 window 分钟前，各类独立冷却）。"""

    window_minutes: int = 10
    limit_up_net_increase: int = 5
    broken_increase: int = 3
    limit_down_net_increase: int = 3
    up_ratio_jump_pct: float = 15.0
    cooldown_minutes: int = 30


def _gi_from_hhmm(t: str) -> int | None:
    try:
        hh, mm = t.split(":")
        return grid_index(dt_time(int(hh), int(mm)))
    except (ValueError, AttributeError):
        return None


class PulseAnomalyWatcher:
    """无 IO 滑窗检测器：喂分钟计数，产出异动。seed 静默回放恢复冷却状态。"""

    def __init__(self, config: PulseConfig | None = None) -> None:
        self.config = config or PulseConfig()
        self._points: list[tuple[int, PulsePoint]] = []  # (grid_idx, point) 升序
        self._last_alert_gi: dict[str, int] = {}

    def seed(self, points: list[PulsePoint]) -> int:
        n = 0
        for p in points:
            gi = _gi_from_hhmm(p.t)
            if gi is None:
                continue
            self._ingest(gi, p, emit=False)
            n += 1
        return n

    def observe(self, point: PulsePoint, now: datetime) -> list[PulseAlert]:
        return self._ingest(grid_index(now.time()), point, emit=True)

    def _reference(self, gi: int) -> PulsePoint | None:
        """最近一个「≥ window 交易分钟前」的点；不存在（预热期）→ None。"""
        cutoff = gi - self.config.window_minutes
        for p_gi, p in reversed(self._points):
            if p_gi <= cutoff:
                return p
        return None

    def _ingest(self, gi: int, point: PulsePoint, emit: bool) -> list[PulseAlert]:
        if self._points and gi <= self._points[-1][0]:
            return []  # 同分钟重复 / 时间回退：不重复记录
        self._points.append((gi, point))
        ref = self._reference(gi)
        if ref is None:
            return []
        w = self.config.window_minutes
        candidates: list[tuple[str, str, float, float, str]] = []
        d_up = point.limit_up - ref.limit_up
        if d_up >= self.config.limit_up_net_increase:
            candidates.append((
                "limit_up_surge", "涨停潮", float(ref.limit_up), float(point.limit_up),
                f"涨停 {w} 分钟 {ref.limit_up} → {point.limit_up}（{d_up:+d}）",
            ))
        d_broken = point.broken - ref.broken
        if d_broken >= self.config.broken_increase:
            candidates.append((
                "broken_surge", "炸板潮", float(ref.broken), float(point.broken),
                f"炸板 {w} 分钟 {ref.broken} → {point.broken}（{d_broken:+d}）",
            ))
        d_down = point.limit_down - ref.limit_down
        if d_down >= self.config.limit_down_net_increase:
            candidates.append((
                "limit_down_surge", "跌停潮", float(ref.limit_down), float(point.limit_down),
                f"跌停 {w} 分钟 {ref.limit_down} → {point.limit_down}（{d_down:+d}）",
            ))
        if point.up_ratio_pct is not None and ref.up_ratio_pct is not None:
            d_ratio = point.up_ratio_pct - ref.up_ratio_pct
            if abs(d_ratio) >= self.config.up_ratio_jump_pct:
                candidates.append((
                    "ratio_jump", "涨跌占比突变",
                    float(ref.up_ratio_pct), float(point.up_ratio_pct),
                    f"上涨占比 {w} 分钟 {ref.up_ratio_pct:.0f}% → "
                    f"{point.up_ratio_pct:.0f}%（{d_ratio:+.0f} pct）",
                ))
        out: list[PulseAlert] = []
        for kind, label, before, after, message in candidates:
            last = self._last_alert_gi.get(kind)
            if last is not None and gi - last < self.config.cooldown_minutes:
                continue
            self._last_alert_gi[kind] = gi  # emit=False 也登记：seed 回放恢复冷却
            if emit:
                out.append(PulseAlert(
                    t=point.t, kind=kind, kind_label=label, before=before,
                    after=after, window_minutes=w, message=message,
                ))
        return out


# ── jsonl IO（append-only；坏行/缺文件降级，不抛异常） ────────────────────────


def pulse_path(live_dir: Path, day: date) -> Path:
    return live_dir / f"{PULSE_FILE_PREFIX}{day.isoformat()}.jsonl"


def alerts_path(live_dir: Path, day: date) -> Path:
    return live_dir / f"{ALERTS_FILE_PREFIX}{day.isoformat()}.jsonl"


def append_jsonl(path: Path, obj: dict) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
    except Exception as e:
        logger.warning(f"pulse 落盘失败（不影响主循环）: {path.name} {type(e).__name__}: {e}")


def read_pulse_points(path: Path) -> list[PulsePoint]:
    if not path.exists():
        return []
    out: list[PulsePoint] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict) and obj.get("t"):
                out.append(PulsePoint(**obj))
        except Exception:
            continue
    return out
