# 全景页爆量图表与脉搏异动 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 爆量记录 tab 点行出图并标注首次触发时刻；脉搏历史由云端 surge-watch 每分钟落盘、📈 浮层改四张分面小图；四类脉搏异动触发页面提示 + PushDeer；爆量检测口径动态展示；分时量柱按分钟涨跌近似上色。

**Architecture:** 云端 surge-watch 主循环（已每分钟拉全市场快照）挂脉搏钩子：算 `MarketPulse` → append `surge_live/pulse-*.jsonl` → 滑窗异动检测 → 推送 + append `pulse_alerts-*.jsonl`，启动时原子写 `runtime_config.json`。全景页只读这些文件渲染，保持纯只读纪律。异动检测核心是无 IO 的 `PulseAnomalyWatcher`（纯内存，可单测）。

**Tech Stack:** Python 3.11 / Pydantic / pandas / Streamlit + Altair / loguru / pytest。

**Spec:** `docs/superpowers/specs/2026-07-29-panorama-surge-pulse-design.md`（同分支，已获用户逐节确认）。

## Global Constraints

- 工作目录（worktree）：`/Users/roxor/brain/30-projects/rQuant/.claude/worktrees/cc+pano-board-surge-pulse`，分支 `cc/pano-board-surge-pulse`。所有命令在此目录执行。
- 测试命令统一：`PYTHONPATH=src /Users/roxor/brain/30-projects/rQuant/.venv/bin/python -m pytest <路径> -v`（PYTHONPATH 保证 import 解析到本 worktree 的 src，已验证）。
- 函数签名全部 type hint；跨层数据结构用 Pydantic，不裸 dict 传递。
- 红涨绿跌（A 股口径）：涨 `#ef4444`、跌 `#10b981`、平/灰 `#94a3b8`、标记橙 `#f97316`。
- UI 层与脉搏钩子**绝不写 DuckDB**；共享文件只用 jsonl append 或 tmp+`os.replace` 原子写；所有读 loader 对文件缺失/坏行降级为空表/None，不抛异常。
- fake 模式 `RQUANT_PANORAMA_FAKE=1` 必须覆盖所有新增 loader（e2e 可测性），fixture 确定性硬编码（唯一例外：fake 异动的时刻取当前时间前 5 分钟，保证提示条在任意时刻可见）。
- Conventional Commits；commit message 结尾加 `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`。
- 本批不改 `deploy/systemd/`、不改 8× 上限与方向门、不做 iTick 看板。

---

### Task 1: pulse_watch 核心（模型 + 滑窗异动检测器 + jsonl IO）

**Files:**
- Create: `src/rquant/pulse_watch.py`
- Test: `tests/unit/test_pulse_watch.py`

**Interfaces:**
- Consumes: `rquant.surge_watch.grid_index(t: dt_time) -> int`（241 交易分钟网格，午休相邻）；`rquant.panorama_data.compute_market_pulse`（Task 2 的 PulseSession 用，本 task 先不引）。
- Produces（后续 task 依赖的确切签名）:
  - `class PulsePoint(BaseModel)`: `t: str`（HH:MM）, `limit_up: int`, `limit_down: int`, `broken: int`, `up: int`, `down: int`, `up_ratio_pct: float | None`, `total: int`
  - `class PulseAlert(BaseModel)`: `t: str`, `kind: str`, `kind_label: str`, `before: float`, `after: float`, `window_minutes: int`, `message: str`
  - `class PulseConfig(BaseModel)`: `window_minutes=10, limit_up_net_increase=5, broken_increase=3, limit_down_net_increase=3, up_ratio_jump_pct=15.0, cooldown_minutes=30`
  - `class PulseAnomalyWatcher`: `__init__(config: PulseConfig | None = None)`, `seed(points: list[PulsePoint]) -> int`, `observe(point: PulsePoint, now: datetime) -> list[PulseAlert]`
  - `pulse_path(live_dir: Path, day: date) -> Path`（`pulse-{day}.jsonl`）、`alerts_path(live_dir: Path, day: date) -> Path`（`pulse_alerts-{day}.jsonl`）、`append_jsonl(path: Path, obj: dict) -> None`、`read_pulse_points(path: Path) -> list[PulsePoint]`

- [ ] **Step 1: 写失败测试**

```python
# tests/unit/test_pulse_watch.py
"""pulse_watch 单测：滑窗四规则 / 冷却 / 预热静默 / seed 续算 / jsonl IO。全离线。"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from datetime import time as dt_time
from pathlib import Path

import pytest

from rquant.pulse_watch import (
    PulseAlert,
    PulseAnomalyWatcher,
    PulseConfig,
    PulsePoint,
    alerts_path,
    append_jsonl,
    pulse_path,
    read_pulse_points,
)

CST = timezone(timedelta(hours=8))


def _pt(t: str, *, limit_up: int = 20, limit_down: int = 2, broken: int = 1,
        up_ratio: float | None = 50.0) -> PulsePoint:
    return PulsePoint(
        t=t, limit_up=limit_up, limit_down=limit_down, broken=broken,
        up=2600, down=2400, up_ratio_pct=up_ratio, total=5400,
    )


def _now(t: str) -> datetime:
    hh, mm = t.split(":")
    return datetime(2026, 7, 29, int(hh), int(mm), tzinfo=CST)


def _feed_flat(w: PulseAnomalyWatcher, start_minute: int, n: int, **kw) -> None:
    """连续喂 n 个平稳分钟（09:30 起算 start_minute 偏移）。"""
    for i in range(n):
        m = 9 * 60 + 30 + start_minute + i
        t = f"{m // 60:02d}:{m % 60:02d}"
        w.observe(_pt(t, **kw), _now(t))


class TestWatcherRules:
    def test_warmup_no_alert_before_window(self) -> None:
        w = PulseAnomalyWatcher()
        # 前 10 分钟内即使涨停暴增也不告警（无 10 分钟前参照点）
        for i, n in enumerate([20, 25, 30, 35, 40]):
            t = f"09:{30 + i:02d}"
            assert w.observe(_pt(t, limit_up=n), _now(t)) == []

    def test_limit_up_surge_triggers_and_cools_down(self) -> None:
        w = PulseAnomalyWatcher()
        _feed_flat(w, 0, 11)  # 09:30..09:40 平稳，建立参照
        alerts = w.observe(_pt("09:41", limit_up=26), _now("09:41"))  # 20→26 净增6≥5
        assert len(alerts) == 1
        a = alerts[0]
        assert a.kind == "limit_up_surge" and a.kind_label == "涨停潮"
        assert a.before == 20 and a.after == 26
        assert "20 → 26" in a.message and "+6" in a.message
        # 冷却 30 分钟内同类不再触发
        assert w.observe(_pt("09:45", limit_up=40), _now("09:45")) == []

    def test_broken_and_limit_down_surge(self) -> None:
        w = PulseAnomalyWatcher()
        _feed_flat(w, 0, 11)
        alerts = w.observe(
            _pt("09:41", broken=4, limit_down=5), _now("09:41")
        )  # 炸板 1→4 (+3) 且跌停 2→5 (+3)
        kinds = {a.kind for a in alerts}
        assert kinds == {"broken_surge", "limit_down_surge"}

    def test_ratio_jump_needs_both_values(self) -> None:
        w = PulseAnomalyWatcher()
        _feed_flat(w, 0, 11, up_ratio=None)  # 参照点无占比 → 不触发
        assert w.observe(_pt("09:41", up_ratio=70.0), _now("09:41")) == []
        w2 = PulseAnomalyWatcher()
        _feed_flat(w2, 0, 11, up_ratio=48.0)
        alerts = w2.observe(_pt("09:41", up_ratio=64.0), _now("09:41"))  # +16pct≥15
        assert [a.kind for a in alerts] == ["ratio_jump"]
        assert "48% → 64%" in alerts[0].message

    def test_same_minute_duplicate_ignored(self) -> None:
        w = PulseAnomalyWatcher()
        _feed_flat(w, 0, 11)
        w.observe(_pt("09:41", limit_up=26), _now("09:41"))
        # 同分钟重复喂（rerun/重试场景）不重复记录也不再告警
        assert w.observe(_pt("09:41", limit_up=30), _now("09:41")) == []

    def test_lunch_gap_uses_trading_minutes(self) -> None:
        w = PulseAnomalyWatcher()
        # 11:20..11:30 平稳（11 点），13:01 相对 11:30 只隔 1 个交易分钟 →
        # 参照点应是 11:21（10 交易分钟前），而非墙钟 12:51
        for i in range(11):
            t = f"11:{20 + i:02d}"
            w.observe(_pt(t), _now(t))
        alerts = w.observe(_pt("13:01", limit_up=26), _now("13:01"))
        assert len(alerts) == 1  # 11:21 的 20 → 13:01 的 26

    def test_seed_restores_history_and_cooldown(self) -> None:
        w = PulseAnomalyWatcher()
        _feed_flat(w, 0, 11)
        assert len(w.observe(_pt("09:41", limit_up=26), _now("09:41"))) == 1
        # 重启：用同样的点序列 seed 新实例（含已触发过告警的分钟）
        points = [_pt(f"09:{30 + i:02d}") for i in range(11)] + [_pt("09:41", limit_up=26)]
        w2 = PulseAnomalyWatcher()
        assert w2.seed(points) == 12
        # seed 静默回放已登记冷却：紧接着的分钟不重复告警
        assert w2.observe(_pt("09:42", limit_up=27), _now("09:42")) == []


class TestJsonlIO:
    def test_paths(self, tmp_path: Path) -> None:
        d = date(2026, 7, 29)
        assert pulse_path(tmp_path, d).name == "pulse-2026-07-29.jsonl"
        assert alerts_path(tmp_path, d).name == "pulse_alerts-2026-07-29.jsonl"

    def test_append_and_read_roundtrip(self, tmp_path: Path) -> None:
        p = pulse_path(tmp_path / "sub", date(2026, 7, 29))  # 自动建目录
        append_jsonl(p, _pt("09:31").model_dump())
        append_jsonl(p, _pt("09:32", limit_up=21).model_dump())
        p.write_text(p.read_text(encoding="utf-8") + "not json\n", encoding="utf-8")
        pts = read_pulse_points(p)  # 坏行跳过
        assert [x.t for x in pts] == ["09:31", "09:32"]
        assert pts[1].limit_up == 21

    def test_read_missing_file_empty(self, tmp_path: Path) -> None:
        assert read_pulse_points(tmp_path / "nope.jsonl") == []
```

- [ ] **Step 2: 跑测试确认失败**

Run: `PYTHONPATH=src /Users/roxor/brain/30-projects/rQuant/.venv/bin/python -m pytest tests/unit/test_pulse_watch.py -v`
Expected: FAIL（`ModuleNotFoundError: No module named 'rquant.pulse_watch'`）

- [ ] **Step 3: 实现 `src/rquant/pulse_watch.py`**

```python
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
```

- [ ] **Step 4: 跑测试确认通过**

Run: `PYTHONPATH=src /Users/roxor/brain/30-projects/rQuant/.venv/bin/python -m pytest tests/unit/test_pulse_watch.py -v`
Expected: 全部 PASS

- [ ] **Step 5: Commit**

```bash
git add src/rquant/pulse_watch.py tests/unit/test_pulse_watch.py
git commit -m "feat(pulse): 脉搏异动滑窗检测器与 jsonl 落盘核心

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: notify 层注册 pulse_alert 场景

**Files:**
- Modify: `src/rquant/notify/api.py`（Scene Literal + `_PUSHDEER_ONLY_SCENES`）
- Modify: `src/rquant/notify/messages.py`（builders 路由）
- Modify: `src/rquant/config.py`（`notify_pulse_alert` 开关，加在 `notify_surge_watch` 下一行）
- Test: `tests/unit/test_notify_messages.py`（追加）

**Interfaces:**
- Produces: `notify("pulse_alert", title=..., body=...)` 可用；场景走 `_build_prerendered`（title/body 由调用方渲染好）；只推 PushDeer（与 surge_watch 同理由：盘中高频提醒只发管理员）。

- [ ] **Step 1: 写失败测试**（追加到 `tests/unit/test_notify_messages.py` 末尾）

```python
def test_pulse_alert_prerendered_passthrough() -> None:
    title, body = build_message(
        "pulse_alert", title="脉搏异动 14:32 炸板潮", body="炸板 10 分钟 2 → 6（+4）"
    )
    assert title == "脉搏异动 14:32 炸板潮"
    assert body == "炸板 10 分钟 2 → 6（+4）"
```

（该文件已有 `from rquant.notify.messages import build_message` 之类导入，沿用；若无则补。）

- [ ] **Step 2: 跑测试确认失败**

Run: `PYTHONPATH=src /Users/roxor/brain/30-projects/rQuant/.venv/bin/python -m pytest tests/unit/test_notify_messages.py -v -k pulse`
Expected: FAIL（unknown scene）

- [ ] **Step 3: 实现三处修改**

`src/rquant/notify/api.py` 的 Scene Literal 追加一项，`_PUSHDEER_ONLY_SCENES` 扩为两项：

```python
Scene = Literal[
    "price_level",
    "pool2_exit",
    "daily_summary",
    "error",
    "heartbeat",
    "morning_pulse",
    "midday_report",
    "surge_watch",
    "pulse_alert",
]

# 只推 admin（PushDeer）不推 PushPlus 的场景：盘中高频/个人盯盘向，只发刘彤
_PUSHDEER_ONLY_SCENES: frozenset[str] = frozenset({"surge_watch", "pulse_alert"})
```

`src/rquant/notify/messages.py` 的 builders 映射（`"surge_watch": _build_prerendered,` 同款）追加：

```python
        "pulse_alert": _build_prerendered,
```

`src/rquant/config.py` 在 `notify_surge_watch: bool = True` 下一行加：

```python
    notify_pulse_alert: bool = True
```

- [ ] **Step 4: 跑测试确认通过**

Run: `PYTHONPATH=src /Users/roxor/brain/30-projects/rQuant/.venv/bin/python -m pytest tests/unit/test_notify_messages.py tests/unit/test_notify_api.py tests/unit/test_config.py -v`
Expected: 全部 PASS（含既有用例不回归）

- [ ] **Step 5: Commit**

```bash
git add src/rquant/notify/api.py src/rquant/notify/messages.py src/rquant/config.py tests/unit/test_notify_messages.py
git commit -m "feat(notify): 新增 pulse_alert 场景（预渲染直通，仅 PushDeer）

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: PulseSession 接线 + surge-watch 主循环挂钩 + runtime_config 落盘

**Files:**
- Modify: `src/rquant/pulse_watch.py`（追加 `PulseSession`）
- Modify: `src/rquant/surge_watch.py`（`write_runtime_config` + `run_surge_watch` 两处挂钩）
- Test: `tests/unit/test_pulse_watch.py`（追加 PulseSession 用例）、`tests/unit/test_surge_watch.py`（追加接线用例）

**Interfaces:**
- Consumes: Task 1 的全部产出；`rquant.panorama_data.compute_market_pulse(snapshot) -> MarketPulse`（字段 `total_count/limit_up_count/limit_down_count/broken_count/up_count/down_count/up_ratio_pct`，快照需列 `price/pre_close/limit_up_price/limit_down_price[/high]`）。
- Produces:
  - `class PulseSession`: `__init__(live_dir: Path, day: date, *, config: PulseConfig | None = None, notify_fn: Callable[..., None] | None = None, dry_run: bool = False)`，`on_snapshot(snapshot: pd.DataFrame, now: datetime) -> list[PulseAlert]`（内部吞异常，绝不拖垮主循环）
  - `surge_watch.RUNTIME_CONFIG_NAME = "runtime_config.json"`、`surge_watch.write_runtime_config(live_dir: Path, config: SurgeConfig, day: date) -> None`
  - 推送报文：title `f"脉搏异动 {t} {kind_label}"`，body `f"{message}｜当前 涨停 X / 跌停 X / 炸板 X / 上涨占比 X%"`

- [ ] **Step 1: 写失败测试**（追加到 `tests/unit/test_pulse_watch.py`）

```python
import pandas as pd

from rquant.pulse_watch import PulseSession


def _pulse_snap(limit_up: int, *, broken: int = 0, total: int = 100) -> pd.DataFrame:
    """构造能让 compute_market_pulse 得出指定涨停/炸板数的最小快照。"""
    rows = []
    for i in range(total):
        pre = 10.0
        cap = 11.0
        if i < limit_up:
            price, high = 11.0, 11.0          # 涨停
        elif i < limit_up + broken:
            price, high = 10.5, 11.0          # 触板回落 = 炸板
        else:
            price, high = 10.2, 10.3          # 普通上涨
        rows.append({
            "ts_code": f"30{i:04d}.SZ", "price": price, "high": high,
            "pre_close": pre, "limit_up_price": cap, "limit_down_price": 9.0,
        })
    return pd.DataFrame(rows)


class TestPulseSession:
    def test_records_points_and_pushes_alert(self, tmp_path: Path) -> None:
        calls: list[tuple] = []
        day = date(2026, 7, 29)
        s = PulseSession(tmp_path, day, notify_fn=lambda scene, **kw: calls.append((scene, kw)))
        for i in range(11):  # 09:30..09:40 平稳
            s.on_snapshot(_pulse_snap(20), _now(f"09:{30 + i:02d}"))
        alerts = s.on_snapshot(_pulse_snap(26), _now("09:41"))  # 涨停 20→26
        assert len(alerts) == 1
        assert pulse_path(tmp_path, day).read_text(encoding="utf-8").count("\n") == 12
        assert alerts_path(tmp_path, day).exists()
        assert calls and calls[0][0] == "pulse_alert"
        assert "脉搏异动 09:41 涨停潮" == calls[0][1]["title"]
        assert "当前 涨停 26" in calls[0][1]["body"]

    def test_dry_run_prints_instead_of_notify(self, tmp_path: Path, capsys) -> None:
        calls: list[tuple] = []
        s = PulseSession(tmp_path, date(2026, 7, 29), dry_run=True,
                         notify_fn=lambda scene, **kw: calls.append((scene, kw)))
        for i in range(11):
            s.on_snapshot(_pulse_snap(20), _now(f"09:{30 + i:02d}"))
        s.on_snapshot(_pulse_snap(26), _now("09:41"))
        assert calls == []
        assert "DRY-RUN" in capsys.readouterr().out

    def test_seed_from_existing_file(self, tmp_path: Path) -> None:
        day = date(2026, 7, 29)
        s1 = PulseSession(tmp_path, day, notify_fn=lambda *a, **k: None)
        for i in range(11):
            s1.on_snapshot(_pulse_snap(20), _now(f"09:{30 + i:02d}"))
        s1.on_snapshot(_pulse_snap(26), _now("09:41"))
        # 重启：新 session 从文件 seed，冷却生效不重复推
        calls: list[tuple] = []
        s2 = PulseSession(tmp_path, day, notify_fn=lambda scene, **kw: calls.append(scene))
        assert s2.on_snapshot(_pulse_snap(27), _now("09:42")) == []
        assert calls == []

    def test_empty_snapshot_skipped(self, tmp_path: Path) -> None:
        s = PulseSession(tmp_path, date(2026, 7, 29), notify_fn=lambda *a, **k: None)
        assert s.on_snapshot(pd.DataFrame(), _now("09:31")) == []
        assert not pulse_path(tmp_path, date(2026, 7, 29)).exists()
```

再追加到 `tests/unit/test_surge_watch.py` 末尾（复用该文件已有的 fake 注入风格；`run_surge_watch` 已导入）：

```python
class TestPulseWiring:
    def test_run_surge_watch_writes_pulse_and_runtime_config(self, tmp_path: Path) -> None:
        from rquant.surge_watch import RUNTIME_CONFIG_NAME, SurgeBaseline, linear_progress_curve

        day = date(2026, 7, 29)
        clock = iter([
            datetime(2026, 7, 29, 9, 31, tzinfo=CST),
            datetime(2026, 7, 29, 9, 32, tzinfo=CST),
            datetime(2026, 7, 29, 9, 33, tzinfo=CST),
            datetime(2026, 7, 29, 9, 34, tzinfo=CST),
        ])
        snap = pd.DataFrame([{
            "ts_code": "300001.SZ", "name": "T1", "price": 10.5, "high": 10.6,
            "pre_close": 10.0, "pct_chg": 5.0, "volume": 1e6, "amount": 1e7,
            "limit_up_price": 12.0, "limit_down_price": 8.0,
        }])
        snap.attrs["route"] = "test"
        run_surge_watch(
            dry_run=True, force_session=True, max_ticks=2, base_dir=tmp_path,
            now_fn=lambda: next(clock), sleep_fn=lambda s: None,
            snapshot_fetcher=lambda: snap.copy(),
            minute_fetcher=lambda code, d: pd.DataFrame(),
            is_trading_day_fn=lambda d: True,
            baseline=SurgeBaseline(avg_amount_20d={}, theme={}, curve=linear_progress_curve()),
        )
        assert (tmp_path / RUNTIME_CONFIG_NAME).exists()
        cfg = json.loads((tmp_path / RUNTIME_CONFIG_NAME).read_text(encoding="utf-8"))
        assert cfg["boards"] == ["gem", "star"] and cfg["ratio_cap"] == 8.0
        pulse_file = tmp_path / f"pulse-{day.isoformat()}.jsonl"
        assert pulse_file.exists()
        assert pulse_file.read_text(encoding="utf-8").count("\n") == 2
```

- [ ] **Step 2: 跑测试确认失败**

Run: `PYTHONPATH=src /Users/roxor/brain/30-projects/rQuant/.venv/bin/python -m pytest tests/unit/test_pulse_watch.py::TestPulseSession tests/unit/test_surge_watch.py::TestPulseWiring -v`
Expected: FAIL（`ImportError: cannot import name 'PulseSession'` / `RUNTIME_CONFIG_NAME`）

- [ ] **Step 3: 实现**

`src/rquant/pulse_watch.py` 顶部 import 区追加：

```python
from collections.abc import Callable

import pandas as pd

from rquant.panorama_data import compute_market_pulse
```

文件末尾追加：

```python
class PulseSession:
    """surge-watch 主循环的脉搏挂钩：算脉搏 → 落历史 → 异动检测 → 推送/落 alerts。

    on_snapshot 内部吞异常只 log——脉搏是旁路，绝不拖垮爆量主循环。
    """

    def __init__(
        self,
        live_dir: Path,
        day: date,
        *,
        config: PulseConfig | None = None,
        notify_fn: Callable[..., None] | None = None,
        dry_run: bool = False,
    ) -> None:
        self.live_dir = live_dir
        self.day = day
        self.dry_run = dry_run
        self.notify_fn = notify_fn
        self.watcher = PulseAnomalyWatcher(config)
        seeded = self.watcher.seed(read_pulse_points(pulse_path(live_dir, day)))
        if seeded:
            logger.info(f"pulse 滑窗 seed {seeded} 分钟（重启续算当日历史）")

    def on_snapshot(self, snapshot: pd.DataFrame, now: datetime) -> list[PulseAlert]:
        try:
            return self._on_snapshot(snapshot, now)
        except Exception as e:
            logger.warning(f"pulse 挂钩异常（不影响主循环）: {type(e).__name__}: {e}")
            return []

    def _on_snapshot(self, snapshot: pd.DataFrame, now: datetime) -> list[PulseAlert]:
        pulse = compute_market_pulse(snapshot)
        if pulse.total_count == 0:
            return []
        point = PulsePoint(
            t=now.strftime("%H:%M"),
            limit_up=pulse.limit_up_count, limit_down=pulse.limit_down_count,
            broken=pulse.broken_count, up=pulse.up_count, down=pulse.down_count,
            up_ratio_pct=pulse.up_ratio_pct, total=pulse.total_count,
        )
        append_jsonl(pulse_path(self.live_dir, self.day), point.model_dump())
        alerts = self.watcher.observe(point, now)
        for a in alerts:
            append_jsonl(alerts_path(self.live_dir, self.day), a.model_dump())
            title = f"脉搏异动 {a.t} {a.kind_label}"
            ratio_txt = f"{point.up_ratio_pct:.0f}%" if point.up_ratio_pct is not None else "—"
            body = (
                f"{a.message}｜当前 涨停 {point.limit_up} / 跌停 {point.limit_down}"
                f" / 炸板 {point.broken} / 上涨占比 {ratio_txt}"
            )
            if self.dry_run or self.notify_fn is None:
                print(f"\n===== [DRY-RUN] {title} =====\n{body}\n")
            else:
                self.notify_fn("pulse_alert", title=title, body=body)
        return alerts
```

`src/rquant/surge_watch.py`：`atomic_write_parquet` 附近追加（模块级，`append_events` 之前）：

```python
RUNTIME_CONFIG_NAME = "runtime_config.json"


def write_runtime_config(live_dir: Path, config: SurgeConfig, day: date) -> None:
    """启动时落生效口径（原子写），供全景页动态展示检测范围。失败只 log。"""
    payload = {
        "day": day.isoformat(),
        "boards": list(config.boards),
        "k_rough": config.k_rough,
        "k_cum": config.k_cum,
        "ratio_cap": config.ratio_cap,
        "skip_first_minutes": config.skip_first_minutes,
        "tushare_rate_per_min": config.tushare_rate_per_min,
        "require_price_strength": config.require_price_strength,
        "max_room_to_limit_pct": config.max_room_to_limit_pct,
    }
    try:
        live_dir.mkdir(parents=True, exist_ok=True)
        tmp = live_dir / (RUNTIME_CONFIG_NAME + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, live_dir / RUNTIME_CONFIG_NAME)
    except Exception as e:
        logger.warning(f"runtime_config 落盘失败（不影响主循环）: {type(e).__name__}: {e}")
```

`run_surge_watch` 里 `events_path = live_dir / f"events-{day.isoformat()}.jsonl"` 之后追加：

```python
    from rquant.pulse_watch import PulseSession  # 函数级导入：pulse_watch 顶层引本模块，避免环

    write_runtime_config(live_dir, config, day)
    pulse_session = PulseSession(live_dir, day, notify_fn=notify_fn, dry_run=dry_run)
```

主循环里 `atomic_write_parquet(full, live_dir / SNAPSHOT_FULL_NAME)` 的下一行追加：

```python
            pulse_session.on_snapshot(full, now)
```

- [ ] **Step 4: 跑测试确认通过**

Run: `PYTHONPATH=src /Users/roxor/brain/30-projects/rQuant/.venv/bin/python -m pytest tests/unit/test_pulse_watch.py tests/unit/test_surge_watch.py -v`
Expected: 全部 PASS（surge_watch 既有用例不回归）

- [ ] **Step 5: Commit**

```bash
git add src/rquant/pulse_watch.py src/rquant/surge_watch.py tests/unit/test_pulse_watch.py tests/unit/test_surge_watch.py
git commit -m "feat(pulse): surge-watch 主循环挂脉搏落盘/异动推送 + 口径 runtime_config

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: panorama_data 新增只读 loader + fake 覆盖

**Files:**
- Modify: `src/rquant/panorama_data.py`（loader 区 + fake 区）
- Test: `tests/unit/test_panorama_pulse_loaders.py`（新建）

**Interfaces:**
- Consumes: Task 1/3 落盘的文件格式（pulse/alerts jsonl、runtime_config.json、既有 events jsonl）。**不 import pulse_watch/surge_watch**（保持 UI 依赖轻量，文件名前缀字面量与 `_SURGE_LIVE_DIR_NAME` 同款做法）。
- Produces:
  - `load_pulse_history(day: date | None = None, *, live_dir: Path | None = None) -> pd.DataFrame` 列 `["t","limit_up","limit_down","broken","up","down","up_ratio_pct","total"]`
  - `load_pulse_alerts(day: date | None = None, *, live_dir: Path | None = None) -> pd.DataFrame` 列 `["t","kind","kind_label","before","after","window_minutes","message"]`
  - `load_surge_runtime_config(*, live_dir: Path | None = None) -> dict | None`
  - `load_surge_marks(ts_code: str, dates: list[date], *, live_dir: Path | None = None) -> pd.DataFrame` 列 `["date","confirmed_at","rel_cum"]`（每天该票最早一行）
  - fake 模式（`RQUANT_PANORAMA_FAKE=1`）：`load_surge_log` 返回 3 条硬编码台账（600001/600002/600003）；pulse history 返回 09:30 起 120 分钟递增序列；alerts 返回 1 条炸板潮（t = 当前时间 − 5 分钟，保证提示条可见）；runtime_config 返回全板块 dict。

- [ ] **Step 1: 写失败测试**（新建 `tests/unit/test_panorama_pulse_loaders.py`）

```python
"""panorama_data 新增 loader 单测：pulse 历史 / 异动 / runtime_config / 爆量标记 + fake 覆盖。"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from rquant.panorama_data import (
    load_pulse_alerts,
    load_pulse_history,
    load_surge_log,
    load_surge_marks,
    load_surge_runtime_config,
)


def _write_lines(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows),
                    encoding="utf-8")


DAY = date(2026, 7, 29)


class TestPulseHistory:
    def test_reads_and_skips_bad_lines(self, tmp_path: Path) -> None:
        p = tmp_path / f"pulse-{DAY.isoformat()}.jsonl"
        _write_lines(p, [
            {"t": "09:31", "limit_up": 20, "limit_down": 2, "broken": 1,
             "up": 2600, "down": 2400, "up_ratio_pct": 50.0, "total": 5400},
            {"t": "09:32", "limit_up": 21, "limit_down": 2, "broken": 1,
             "up": 2610, "down": 2390, "up_ratio_pct": 50.2, "total": 5400},
        ])
        with p.open("a", encoding="utf-8") as f:
            f.write("BROKEN\n{\"no_t\": 1}\n")
        df = load_pulse_history(DAY, live_dir=tmp_path)
        assert list(df["t"]) == ["09:31", "09:32"]
        assert df.iloc[1]["limit_up"] == 21

    def test_missing_file_empty_with_columns(self, tmp_path: Path) -> None:
        df = load_pulse_history(DAY, live_dir=tmp_path)
        assert df.empty and "limit_up" in df.columns


class TestPulseAlerts:
    def test_reads_alerts(self, tmp_path: Path) -> None:
        p = tmp_path / f"pulse_alerts-{DAY.isoformat()}.jsonl"
        _write_lines(p, [{
            "t": "10:15", "kind": "broken_surge", "kind_label": "炸板潮",
            "before": 2, "after": 6, "window_minutes": 10,
            "message": "炸板 10 分钟 2 → 6（+4）",
        }])
        df = load_pulse_alerts(DAY, live_dir=tmp_path)
        assert len(df) == 1 and df.iloc[0]["kind_label"] == "炸板潮"

    def test_missing_file_empty(self, tmp_path: Path) -> None:
        assert load_pulse_alerts(DAY, live_dir=tmp_path).empty


class TestRuntimeConfig:
    def test_reads_config(self, tmp_path: Path) -> None:
        (tmp_path / "runtime_config.json").write_text(
            json.dumps({"boards": ["main", "gem"], "k_cum": 2.5, "ratio_cap": 8.0}),
            encoding="utf-8",
        )
        cfg = load_surge_runtime_config(live_dir=tmp_path)
        assert cfg is not None and cfg["boards"] == ["main", "gem"]

    def test_missing_or_broken_none(self, tmp_path: Path) -> None:
        assert load_surge_runtime_config(live_dir=tmp_path) is None
        (tmp_path / "runtime_config.json").write_text("nope", encoding="utf-8")
        assert load_surge_runtime_config(live_dir=tmp_path) is None


class TestSurgeMarks:
    def test_earliest_per_day_and_missing_days_skipped(self, tmp_path: Path) -> None:
        d1, d2, d3 = date(2026, 7, 27), date(2026, 7, 28), date(2026, 7, 29)
        _write_lines(tmp_path / f"events-{d1.isoformat()}.jsonl", [
            {"ts_code": "688255.SH", "confirmed_at": "10:31", "rel_cum": 4.0},
            {"ts_code": "688255.SH", "confirmed_at": "09:47", "rel_cum": 3.2},
            {"ts_code": "300409.SZ", "confirmed_at": "09:40", "rel_cum": 2.8},
        ])
        _write_lines(tmp_path / f"events-{d3.isoformat()}.jsonl", [
            {"ts_code": "688255.SH", "confirmed_at": "13:05", "rel_cum": 5.5},
        ])
        df = load_surge_marks("688255.SH", [d1, d2, d3], live_dir=tmp_path)
        assert list(df["confirmed_at"]) == ["09:47", "13:05"]  # d2 无文件跳过
        assert list(df["date"]) == [d1, d3]
        assert df.iloc[0]["rel_cum"] == pytest.approx(3.2)

    def test_no_hit_empty(self, tmp_path: Path) -> None:
        df = load_surge_marks("000001.SZ", [DAY], live_dir=tmp_path)
        assert df.empty and list(df.columns) == ["date", "confirmed_at", "rel_cum"]


class TestFakeMode:
    def test_fake_covers_all_new_loaders(self, monkeypatch) -> None:
        monkeypatch.setenv("RQUANT_PANORAMA_FAKE", "1")
        hist = load_pulse_history()
        assert len(hist) >= 60 and hist.iloc[0]["t"] == "09:30"
        alerts = load_pulse_alerts()
        assert len(alerts) == 1 and alerts.iloc[0]["kind"] == "broken_surge"
        cfg = load_surge_runtime_config()
        assert cfg is not None and set(cfg["boards"]) == {"main", "gem", "star", "bj"}
        log = load_surge_log()
        assert len(log) == 3 and "600001.SH" in set(log["ts_code"])
        marks = load_surge_marks("600001.SH", [date.today()])
        assert len(marks) == 1
```

- [ ] **Step 2: 跑测试确认失败**

Run: `PYTHONPATH=src /Users/roxor/brain/30-projects/rQuant/.venv/bin/python -m pytest tests/unit/test_panorama_pulse_loaders.py -v`
Expected: FAIL（ImportError: cannot import name 'load_pulse_history'）

- [ ] **Step 3: 实现**

`src/rquant/panorama_data.py` 在 `load_surge_log` 附近（`_SURGE_LIVE_DIR_NAME` 区）追加常量与通用读取，并给 `load_surge_log` 加 fake 分支：

```python
_PULSE_LOG_COLUMNS = ["t", "limit_up", "limit_down", "broken", "up", "down",
                      "up_ratio_pct", "total"]
_PULSE_ALERT_COLUMNS = ["t", "kind", "kind_label", "before", "after",
                        "window_minutes", "message"]
_RUNTIME_CONFIG_NAME = "runtime_config.json"


def _surge_live_dir(live_dir: Path | None) -> Path:
    if live_dir is not None:
        return live_dir
    from rquant.config import settings

    return settings.data_dir / _SURGE_LIVE_DIR_NAME


def _read_jsonl_records(path: Path, required_key: str) -> list[dict]:
    """逐行读 jsonl，跳过坏行/缺关键字段的行；文件缺失 → 空。"""
    if not path.exists():
        return []
    records: list[dict] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except (json.JSONDecodeError, ValueError):
            continue
        if isinstance(obj, dict) and obj.get(required_key):
            records.append(obj)
    return records


def load_pulse_history(
    day: date | None = None, *, live_dir: Path | None = None
) -> pd.DataFrame:
    """当日脉搏分钟历史（surge-watch 落盘）。缺失/坏行降级，列契约恒定。"""
    if _fake_enabled():
        return _fake_pulse_history()
    if day is None:
        day = datetime.now(_CST).date()
    path = _surge_live_dir(live_dir) / f"pulse-{day.isoformat()}.jsonl"
    records = _read_jsonl_records(path, "t")
    if not records:
        return pd.DataFrame(columns=_PULSE_LOG_COLUMNS)
    df = pd.DataFrame(records)
    for col in _PULSE_LOG_COLUMNS:
        if col not in df.columns:
            df[col] = float("nan")
    return df[_PULSE_LOG_COLUMNS]


def load_pulse_alerts(
    day: date | None = None, *, live_dir: Path | None = None
) -> pd.DataFrame:
    """当日脉搏异动事件（surge-watch 落盘），按文件顺序（即时间序）。"""
    if _fake_enabled():
        return _fake_pulse_alerts()
    if day is None:
        day = datetime.now(_CST).date()
    path = _surge_live_dir(live_dir) / f"pulse_alerts-{day.isoformat()}.jsonl"
    records = _read_jsonl_records(path, "t")
    if not records:
        return pd.DataFrame(columns=_PULSE_ALERT_COLUMNS)
    df = pd.DataFrame(records)
    for col in _PULSE_ALERT_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    return df[_PULSE_ALERT_COLUMNS]


def load_surge_runtime_config(*, live_dir: Path | None = None) -> dict | None:
    """surge-watch 当前生效口径（启动时原子写）。缺失/损坏 → None。"""
    if _fake_enabled():
        return {"boards": ["main", "gem", "star", "bj"], "k_rough": 1.2,
                "k_cum": 2.5, "ratio_cap": 8.0, "tushare_rate_per_min": 2}
    path = _surge_live_dir(live_dir) / _RUNTIME_CONFIG_NAME
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def load_surge_marks(
    ts_code: str, dates: list[date], *, live_dir: Path | None = None
) -> pd.DataFrame:
    """指定交易日集合内该票每天最早爆量确认（图表标记源）。

    逐日复用 load_surge_log（每票保留最早行的语义一致）；某日文件缺失自然跳过。
    """
    rows: list[dict] = []
    for d in dates:
        df = load_surge_log(d, live_dir=live_dir)
        if df.empty or "ts_code" not in df.columns:
            continue
        sub = df[df["ts_code"].astype(str) == ts_code]
        if sub.empty:
            continue
        r = sub.iloc[0]
        rows.append({
            "date": d,
            "confirmed_at": str(r.get("confirmed_at", "")),
            "rel_cum": pd.to_numeric(pd.Series([r.get("rel_cum")]), errors="coerce").iloc[0],
        })
    return pd.DataFrame(rows, columns=["date", "confirmed_at", "rel_cum"])
```

`load_surge_log` 函数体开头（`if day is None:` 之前）加 fake 分支：

```python
    if _fake_enabled():
        return _fake_surge_log()
```

同时把 `load_surge_log` 的默认目录解析改用 `_surge_live_dir(live_dir)`（行为不变的小重构），其行内 jsonl 解析循环替换为 `records = _read_jsonl_records(path, "ts_code")`（保持「缺 ts_code 跳过」语义）。

fake 区（`_fake_daily_kline` 之后）追加：

```python
def _fake_surge_log() -> pd.DataFrame:
    """3 条确定性爆量台账：与 _FAKE_CODES 网格对齐（600001 涨停、600003 炸板）。"""
    rows = [
        {"confirmed_at": "09:47", "ts_code": "600001.SH", "name": "假票600001",
         "theme": "人形机器人", "price": 11.0, "pct_chg": 10.0, "rel_cum": 3.2,
         "cum_amount": 5.2e8, "room_to_limit_pct": 0.0, "status": "unbuyable"},
        {"confirmed_at": "10:12", "ts_code": "600003.SH", "name": "假票600003",
         "theme": "人形机器人", "price": 10.4, "pct_chg": 4.0, "rel_cum": 5.1,
         "cum_amount": 3.1e8, "room_to_limit_pct": 5.8, "status": "confirmed"},
        {"confirmed_at": "13:05", "ts_code": "600010.SH", "name": "假票600010",
         "theme": "算力", "price": 12.6, "pct_chg": 6.3, "rel_cum": 2.7,
         "cum_amount": 2.4e8, "room_to_limit_pct": 3.5, "status": "confirmed"},
    ]
    return pd.DataFrame(rows)


def _fake_pulse_history() -> pd.DataFrame:
    """09:30 起 120 分钟确定性脉搏序列（涨停缓升、炸板阶梯、占比爬升）。"""
    rows: list[dict] = []
    minute = 9 * 60 + 30
    for i in range(120):
        rows.append({
            "t": f"{minute // 60:02d}:{minute % 60:02d}",
            "limit_up": 20 + i // 6, "limit_down": 3 + i // 40, "broken": 2 + i // 15,
            "up": 2600 + i * 5, "down": 2400 - i * 5,
            "up_ratio_pct": round(48 + i * 0.1, 2), "total": 5400,
        })
        minute += 1
        if minute == 11 * 60 + 31:
            minute = 13 * 60 + 1
    return pd.DataFrame(rows, columns=_PULSE_LOG_COLUMNS)


def _fake_pulse_alerts() -> pd.DataFrame:
    """1 条炸板潮；t 取当前时间 − 5 分钟（唯一非硬编码字段，保证提示条恒可见）。"""
    t = (datetime.now(_CST) - timedelta(minutes=5)).strftime("%H:%M")
    return pd.DataFrame([{
        "t": t, "kind": "broken_surge", "kind_label": "炸板潮",
        "before": 2.0, "after": 6.0, "window_minutes": 10,
        "message": "炸板 10 分钟 2 → 6（+4）",
    }], columns=_PULSE_ALERT_COLUMNS)
```

（`panorama_data.py` 顶部已有 `json` / `datetime` / `timedelta` / `_CST` / `Path`；若 `timedelta` 未导入则在 datetime 导入行补上。）

- [ ] **Step 4: 跑测试确认通过**

Run: `PYTHONPATH=src /Users/roxor/brain/30-projects/rQuant/.venv/bin/python -m pytest tests/unit/test_panorama_pulse_loaders.py tests/unit/test_panorama_surge_log.py tests/unit/test_panorama_data.py -v`
Expected: 全部 PASS（load_surge_log 重构不回归）

- [ ] **Step 5: Commit**

```bash
git add src/rquant/panorama_data.py tests/unit/test_panorama_pulse_loaders.py
git commit -m "feat(panorama): pulse/alerts/runtime_config/标记 只读 loader + fake 覆盖

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: panorama_data 纯 helper：标记定位 + 量柱方向

**Files:**
- Modify: `src/rquant/panorama_data.py`（helper 区，`fetch_intraday_trend` 之后）
- Test: `tests/unit/test_panorama_pulse_loaders.py`（追加两个测试类）

**Interfaces:**
- Consumes: trend DataFrame（列 `dt/price/avg_price/volume`）、Task 4 的 marks DataFrame（列 `date/confirmed_at/rel_cum`）。
- Produces:
  - `surge_mark_positions(trend: pd.DataFrame, marks: pd.DataFrame) -> pd.DataFrame` 列 `["idx","price","label"]`（idx = trend 行位置；精确分钟缺失回退同日 ≤ 时刻最近一根；当日无数据跳过）
  - `volume_directions(prices: pd.Series) -> pd.Series`（值 `"up"/"down"/"flat"`，首根 flat）

- [ ] **Step 1: 写失败测试**（追加到 `tests/unit/test_panorama_pulse_loaders.py`）

```python
import pandas as pd

from rquant.panorama_data import surge_mark_positions, volume_directions


def _trend(day: str, times: list[str], prices: list[float]) -> pd.DataFrame:
    return pd.DataFrame({
        "dt": pd.to_datetime([f"{day} {t}" for t in times]),
        "price": prices,
        "avg_price": [float("nan")] * len(times),
        "volume": [100.0] * len(times),
    })


class TestSurgeMarkPositions:
    def test_exact_minute_hit(self) -> None:
        trend = _trend("2026-07-29", ["09:46", "09:47", "09:48"], [10.0, 10.5, 10.6])
        marks = pd.DataFrame([{"date": date(2026, 7, 29), "confirmed_at": "09:47",
                               "rel_cum": 3.2}])
        pos = surge_mark_positions(trend, marks)
        assert len(pos) == 1
        assert pos.iloc[0]["idx"] == 1 and pos.iloc[0]["price"] == pytest.approx(10.5)
        assert pos.iloc[0]["label"] == "09:47 首次爆量确认 · 3.2×"

    def test_missing_minute_falls_back_to_prior_bar(self) -> None:
        trend = _trend("2026-07-29", ["09:46", "09:49"], [10.0, 10.6])
        marks = pd.DataFrame([{"date": date(2026, 7, 29), "confirmed_at": "09:47",
                               "rel_cum": float("nan")}])
        pos = surge_mark_positions(trend, marks)
        assert pos.iloc[0]["idx"] == 0
        assert pos.iloc[0]["label"] == "09:47 首次爆量确认"  # rel_cum 缺失不带倍数

    def test_day_absent_skipped_and_empty_inputs(self) -> None:
        trend = _trend("2026-07-29", ["09:46"], [10.0])
        marks = pd.DataFrame([{"date": date(2026, 7, 28), "confirmed_at": "09:47",
                               "rel_cum": 2.0}])
        assert surge_mark_positions(trend, marks).empty
        assert surge_mark_positions(trend, pd.DataFrame()).empty
        assert surge_mark_positions(pd.DataFrame(), marks).empty


class TestVolumeDirections:
    def test_directions(self) -> None:
        prices = pd.Series([10.0, 10.2, 10.2, 10.1])
        assert list(volume_directions(prices)) == ["flat", "up", "flat", "down"]
```

- [ ] **Step 2: 跑测试确认失败**

Run: `PYTHONPATH=src /Users/roxor/brain/30-projects/rQuant/.venv/bin/python -m pytest tests/unit/test_panorama_pulse_loaders.py -v -k "MarkPositions or VolumeDirections"`
Expected: FAIL（ImportError）

- [ ] **Step 3: 实现**（`panorama_data.py`，`load_daily_kline` 之前）

```python
def surge_mark_positions(trend: pd.DataFrame, marks: pd.DataFrame) -> pd.DataFrame:
    """爆量标记时刻 → trend 行位置（列 idx/price/label），供图层画竖线+标记点。

    精确分钟缺失（数据缺根）回退同日 ≤ 时刻的最近一根；当日无数据跳过该标记。
    """
    cols = ["idx", "price", "label"]
    if trend is None or trend.empty or marks is None or marks.empty:
        return pd.DataFrame(columns=cols)
    dt = pd.to_datetime(trend["dt"]).reset_index(drop=True)
    prices = pd.to_numeric(trend["price"], errors="coerce").reset_index(drop=True)
    rows: list[dict] = []
    for m in marks.itertuples():
        try:
            hh, mm = str(m.confirmed_at).split(":")
            target = pd.Timestamp(m.date).replace(hour=int(hh), minute=int(mm))
        except (ValueError, AttributeError, TypeError):
            continue
        same_day = dt.dt.normalize() == target.normalize()
        candidates = dt[same_day & (dt <= target)]
        if candidates.empty:
            continue
        idx = int(candidates.index[-1])
        label = f"{m.confirmed_at} 首次爆量确认"
        rel = getattr(m, "rel_cum", None)
        if rel is not None and not pd.isna(rel):
            label += f" · {float(rel):.1f}×"
        rows.append({"idx": idx, "price": float(prices.iloc[idx]), "label": label})
    return pd.DataFrame(rows, columns=cols)


def volume_directions(prices: pd.Series) -> pd.Series:
    """每分钟方向（tick-rule 近似）：收涨 up / 收跌 down / 平或首根 flat。"""
    diff = pd.to_numeric(prices, errors="coerce").diff()
    out = pd.Series("flat", index=prices.index, dtype="object")
    out[diff > 0] = "up"
    out[diff < 0] = "down"
    return out
```

- [ ] **Step 4: 跑测试确认通过**

Run: `PYTHONPATH=src /Users/roxor/brain/30-projects/rQuant/.venv/bin/python -m pytest tests/unit/test_panorama_pulse_loaders.py -v`
Expected: 全部 PASS

- [ ] **Step 5: Commit**

```bash
git add src/rquant/panorama_data.py tests/unit/test_panorama_pulse_loaders.py
git commit -m "feat(panorama): 爆量标记定位与量柱方向纯 helper

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: UI — 图表组件挂标记 + 量柱上色 + key 参数化

**Files:**
- Modify: `src/rquant/dashboard/market_panorama.py`（import 区、`cached_surge_log` 附近、`_trend_chart`、`render_stock_chart`）

**Interfaces:**
- Consumes: Task 4 `load_surge_marks`、Task 5 `surge_mark_positions` / `volume_directions`。
- Produces: `render_stock_chart(ts_code, name, snapshot, *, key_prefix: str = "pano") -> None`（Task 7 爆量 tab 以 `key_prefix="surge"` 调用）；`_trend_chart(trend, marks: pd.DataFrame | None = None)`。

无独立单测（纯 Streamlit 渲染层，逻辑已在 Task 4/5 单测覆盖；页面验证在 Task 9 e2e）。

- [ ] **Step 1: 修改 import 与缓存**

import 区补：

```python
from datetime import date  # 已有 datetime/timedelta，补 date
```

`from rquant.panorama_data import (...)` 追加：`load_pulse_alerts, load_pulse_history, load_surge_marks, load_surge_runtime_config, surge_mark_positions, volume_directions`。

`cached_surge_log` 之后追加缓存：

```python
@st.cache_data(ttl=60, show_spinner=False)
def cached_surge_marks(ts_code: str, dates_key: str) -> pd.DataFrame:
    """图表标记（键 = 票 + 交易日集合字符串；日集合来自 trend 实际数据）。"""
    dates = [date.fromisoformat(s) for s in dates_key.split(",") if s]
    return load_surge_marks(ts_code, dates)
```

- [ ] **Step 2: `_trend_chart` 加标记层与量柱上色**

签名改 `def _trend_chart(trend: pd.DataFrame, marks: pd.DataFrame | None = None) -> alt.VConcatChart:`；`layers = [price_line]` 与 avg 线逻辑之后、`price = alt.layer(...)` 之前插入：

```python
    mark_pos = (
        surge_mark_positions(trend, marks)
        if marks is not None and not marks.empty else pd.DataFrame()
    )
    if not mark_pos.empty:
        mark_tip = [alt.Tooltip("label:N", title="爆量")]
        layers.append(
            alt.Chart(mark_pos).mark_rule(
                color="#f97316", strokeDash=[6, 4], size=2
            ).encode(x=alt.X("idx:Q", scale=x_scale), tooltip=mark_tip)
        )
        layers.append(
            alt.Chart(mark_pos).mark_point(
                color="#f97316", filled=True, size=80
            ).encode(x=alt.X("idx:Q", scale=x_scale), y="price:Q", tooltip=mark_tip)
        )
```

量柱：`trend = trend.reset_index(...)` 行后追加一行方向色列，`vol` 图层的 `mark_bar(color="#94a3b8")` 改为方向色编码：

```python
    trend["vol_color"] = volume_directions(trend["price"]).map(
        {"up": _UP_COLOR, "down": _DOWN_COLOR, "flat": "#94a3b8"}
    )
```

```python
    vol = (
        alt.Chart(trend)
        .mark_bar()
        .encode(
            x=x_vol,
            y=alt.Y("volume:Q", title=None),
            color=alt.Color("vol_color:N", scale=None),
            tooltip=[dt_tip, alt.Tooltip("volume:Q", title="量")],
        )
        .properties(height=70)
    )
```

- [ ] **Step 3: `render_stock_chart` 参数化 key + 取标记**

签名改 `def render_stock_chart(ts_code: str | None, name: str | None, snapshot: pd.DataFrame, *, key_prefix: str = "pano") -> None:`；`segmented_control` 的 `key="chart_period"` 改 `key=f"chart_period_{key_prefix}"`；分时/5日分支改为：

```python
    if period in ("分时", "5日"):
        ndays = 1 if period == "分时" else 5
        trend, route = cached_trend(ts_code, ndays)
        if trend.empty:
            st.info(f"{period}数据暂不可用")
            return
        days = sorted(pd.to_datetime(trend["dt"]).dt.date.unique())
        marks = cached_surge_marks(ts_code, ",".join(d.isoformat() for d in days))
        st.altair_chart(_trend_chart(trend, marks), width="stretch")
        st.caption(
            f"数据路由：{ROUTE_LABELS.get(route, route)}"
            " · 量柱色=分钟涨跌近似（tick-rule），非真实内外盘"
            " · 橙线=首次爆量确认"
        )
```

- [ ] **Step 4: 冒烟验证（fake 模式起页面确认不报错）**

```bash
RQUANT_PANORAMA_FAKE=1 PYTHONPATH=src /Users/roxor/brain/30-projects/rQuant/.venv/bin/python -m streamlit run src/rquant/dashboard/market_panorama.py --server.port 8516 --server.headless true &
sleep 8 && curl -s http://localhost:8516 | head -3 && kill %1
```

Expected: 返回 HTML（`<!DOCTYPE html>` 开头），终端无 traceback。

- [ ] **Step 5: Commit**

```bash
git add src/rquant/dashboard/market_panorama.py
git commit -m "feat(panorama-ui): 个股图表爆量标记层 + 量柱方向近似上色 + 组件 key 参数化

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 7: UI — 爆量记录 tab 行选择出图 + 口径动态页脚

**Files:**
- Modify: `src/rquant/dashboard/market_panorama.py`（`render_surge_log`、`render_body`、`cached_runtime_config`）

**Interfaces:**
- Consumes: Task 6 的 `render_stock_chart(..., key_prefix="surge")`、Task 4 `load_surge_runtime_config`、既有 `_first_selected_row` / `cached_surge_log`。
- Produces: `render_surge_log(snapshot: pd.DataFrame) -> None`（签名变化，`render_body` 调用处同步改）。

- [ ] **Step 1: 实现**

缓存区追加：

```python
@st.cache_data(ttl=300, show_spinner=False)
def cached_runtime_config() -> dict | None:
    return load_surge_runtime_config()
```

`render_surge_log` 整体替换为：

```python
_BOARD_LABELS = {"main": "主板", "gem": "创业", "star": "科创", "bj": "北交"}


def _surge_caption(n_rows: int) -> str:
    """页脚口径：优先 runtime_config 动态展示，缺失退回写死文案。"""
    cfg = cached_runtime_config()
    if cfg:
        boards = "/".join(_BOARD_LABELS.get(b, str(b)) for b in cfg.get("boards", []))
        return (
            f"检测范围：{boards or '—'}"
            f" · 口径 v4：累计放量 {cfg.get('k_cum', '—')}-{cfg.get('ratio_cap', '—')}×"
            " + 当前分钟上涨 + 外盘占优（tick-rule 近似）"
            " · 每标的取当日最早识别时刻"
            f" · 观察提示非买入信号 · 共 {n_rows} 条"
        )
    return (
        "口径 v4：累计放量 + 当前分钟上涨 + 外盘占优（tick-rule 近似）"
        " · 每标的取当日最早识别时刻"
        f" · 观察提示非买入信号 · 共 {n_rows} 条"
    )


def render_surge_log(snapshot: pd.DataFrame) -> None:
    """当日爆量台账：行选择联动下方个股图表（分时/5日带首次触发标记）。"""
    today = datetime.now(CST).date()
    df = cached_surge_log(today.isoformat())
    if df.empty:
        st.info("今日暂无爆量记录（surge-watch 尚未识别到，或未到盘中）")
        return
    event = st.dataframe(
        _surge_log_display(df),
        key="surge_tbl",
        on_select="rerun",
        selection_mode="single-row",
        hide_index=True,
        width="stretch",
        height=300,
    )
    st.caption(_surge_caption(len(df)))
    idx = _first_selected_row(event)
    if idx is None or idx >= len(df):
        st.info("点选记录查看个股图表（分时/5日图标注首次爆量触发时刻）")
        return
    row = df.iloc[idx]
    render_stock_chart(
        str(row["ts_code"]), str(row.get("name", "")), snapshot, key_prefix="surge"
    )
```

`render_body` 里 `render_surge_log()` 调用改为 `render_surge_log(snapshot)`。

- [ ] **Step 2: 冒烟验证**（同 Task 6 Step 4 命令）

Expected: 页面可起、无 traceback。

- [ ] **Step 3: Commit**

```bash
git add src/rquant/dashboard/market_panorama.py
git commit -m "feat(panorama-ui): 爆量记录行选择联动个股图表 + 检测口径动态页脚

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 8: UI — 📈 浮层分面小图 + 异动提示条/toast

**Files:**
- Modify: `src/rquant/dashboard/market_panorama.py`（`render_pulse` 及缓存）

**Interfaces:**
- Consumes: Task 4 `load_pulse_history` / `load_pulse_alerts`；既有 `_record_pulse_history`（保留作本地兜底）。
- Produces: 浮层四张分面小图（涨停红/炸板橙/跌停绿/上涨占比蓝，独立 y 轴 `zero=False`）；脉搏行下方最近 30 分钟异动 `st.warning` + 新异动一次性 `st.toast`。

- [ ] **Step 1: 实现**

缓存区追加：

```python
@st.cache_data(ttl=60, show_spinner=False)
def cached_pulse_history(day_key: str) -> pd.DataFrame:
    return load_pulse_history()


@st.cache_data(ttl=60, show_spinner=False)
def cached_pulse_alerts(day_key: str) -> pd.DataFrame:
    return load_pulse_alerts()
```

`render_pulse` 之前追加分面图构造与提示条：

```python
_PULSE_FACETS: list[tuple[str, str, str]] = [
    ("limit_up", "涨停", _UP_COLOR),
    ("broken", "炸板", "#f97316"),
    ("limit_down", "跌停", _DOWN_COLOR),
    ("up_ratio_pct", "上涨占比%", "#2563eb"),
]


def _pulse_facet_chart(hist: pd.DataFrame) -> alt.VConcatChart:
    """四指标分面小图：独立 y 轴且不从 0 起，x 轴共享、约 6 个稀疏刻度。"""
    ticks = hist["t"].tolist()[:: max(1, len(hist) // 6)]
    x = alt.X("t:O", title=None, axis=alt.Axis(values=ticks, labelAngle=0))
    rows = [
        alt.Chart(hist).mark_line(color=color).encode(
            x=x,
            y=alt.Y(f"{col}:Q", title=title, scale=alt.Scale(zero=False)),
            tooltip=["t", alt.Tooltip(f"{col}:Q", title=title)],
        ).properties(height=64)
        for col, title, color in _PULSE_FACETS
    ]
    return alt.vconcat(*rows).resolve_scale(x="shared", y="independent")


def _render_pulse_alert_line(now: datetime) -> None:
    """最近 30 分钟内的异动：常驻 warning + 新异动一次性 toast（会话内去重）。"""
    alerts = cached_pulse_alerts(now.date().isoformat())
    if alerts.empty:
        return
    latest = alerts.iloc[-1]
    try:
        hh, mm = str(latest["t"]).split(":")
        alert_dt = now.replace(hour=int(hh), minute=int(mm), second=0, microsecond=0)
    except ValueError:
        return
    if not (timedelta(0) <= now - alert_dt <= timedelta(minutes=30)):
        return
    extra = f"（今日共 {len(alerts)} 次异动）" if len(alerts) > 1 else ""
    st.warning(f"⚡ {latest['t']} {latest['kind_label']}：{latest['message']}{extra}")
    seen: set[str] = st.session_state.setdefault("seen_pulse_alerts", set())
    key = f"{latest['t']}-{latest['kind']}"
    if key not in seen:
        seen.add(key)
        st.toast(f"⚡ {latest['t']} {latest['kind_label']}：{latest['message']}")
```

`render_pulse` 内改动两处：`st.caption(_snapshot_status_line(...))` 之后加一行 `_render_pulse_alert_line(datetime.now(CST))`；popover 内容替换为：

```python
    with c6, st.popover("📈", width="stretch"):
        st.caption(f"快照 {as_of} · 有效样本 {pulse.total_count} 只（停牌除外）")
        hist = cached_pulse_history(datetime.now(CST).date().isoformat())
        if len(hist) >= 2:
            st.altair_chart(_pulse_facet_chart(hist), width="stretch")
            st.caption("数据来源：服务端全天历史（surge-watch 每分钟落盘）")
        elif not spark.empty and spark["time"].nunique() >= 2:
            chart = (
                alt.Chart(spark)
                .mark_line(point=True)
                .encode(
                    x=alt.X("time:O", title=None),
                    y=alt.Y("家数:Q", title=None, scale=alt.Scale(zero=False)),
                    color=alt.Color("指标:N", legend=alt.Legend(orient="top", title=None)),
                    tooltip=["time", "指标", "家数"],
                )
                .properties(height=160)
            )
            st.altair_chart(chart, width="stretch")
            st.caption("数据来源：本会话累积（服务端历史不可用，本地兜底）")
        else:
            st.caption("脉搏曲线累积中（需 ≥2 分钟样本）")
```

（`_record_pulse_history` 调用保留不动——它是本地兜底数据源。）

- [ ] **Step 2: 冒烟验证**（同 Task 6 Step 4 命令）

Expected: 页面可起、无 traceback。

- [ ] **Step 3: Commit**

```bash
git add src/rquant/dashboard/market_panorama.py
git commit -m "feat(panorama-ui): 脉搏浮层四分面小图 + 异动提示条与 toast

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 9: 全量回归 + Playwright e2e 自测 + CHANGELOG + PR

**Files:**
- Modify: `CHANGELOG.md`（`[Unreleased]` section）

- [ ] **Step 1: 全量单测回归**

Run: `PYTHONPATH=src /Users/roxor/brain/30-projects/rQuant/.venv/bin/python -m pytest tests/unit -x -q`
Expected: 全部 PASS

- [ ] **Step 2: fake 模式起页面 + Playwright e2e 自测**

```bash
RQUANT_PANORAMA_FAKE=1 PYTHONPATH=src /Users/roxor/brain/30-projects/rQuant/.venv/bin/python -m streamlit run src/rquant/dashboard/market_panorama.py --server.port 8516 --server.headless true
```

（后台起，用 Playwright MCP 工具驱动 `http://localhost:8516`）e2e checklist——happy path 与边界都要过，截图留档：

1. 市场全景 tab：脉搏 metric 出数；异动提示条可见（fake 恒有一条炸板潮，⚡ + 「炸板潮」文案）。
2. 📈 popover 打开：四张分面小图（涨停/炸板/跌停/上涨占比），y 轴非零起点。
3. 爆量记录 tab：表格 3 行（600001/600003/600010），页脚出现「检测范围：主板/创业/科创/北交」。
4. 点选第一行（600001）：下方出个股图表；分时图有橙色竖直虚线 + 标记点。
5. 周期切到 5日：橙色标记仍在（fake 台账当日）；切日K：无标记、正常渲染。
6. 未选行状态：显示「点选记录查看个股图表」提示（先切走再切回验证）。
7. 两个 tab 各自的周期切换控件互不干扰（key 参数化验证：市场全景选「日K」后，爆量 tab 仍默认「分时」）。
8. Console 无未捕获错误。

完成后 kill streamlit 进程。任何一条不过 → 修复后重跑本 checklist。

- [ ] **Step 3: 更新 CHANGELOG**

`CHANGELOG.md` 的 `[Unreleased]` 下追加：

```markdown
### Added
- 爆量记录 tab 行选择联动个股图表，分时/5日图橙色虚线标注每日首次爆量确认时刻（悬停显示时间与倍数）
- 脉搏历史服务端化：surge-watch 每分钟落 `surge_live/pulse-*.jsonl`，📈 浮层改四张分面小图（涨停/炸板/跌停/上涨占比，独立 y 轴）
- 脉搏异动检测（涨停潮/炸板潮/跌停潮/涨跌占比突变，10 分钟滑窗 + 30 分钟冷却）：页面提示条 + PushDeer `pulse_alert` 场景推送
- surge-watch 启动落 `runtime_config.json`，爆量记录页脚动态显示检测范围与口径

### Changed
- 分时/5日量柱按分钟涨跌近似上色（tick-rule，红涨绿跌，页脚注明近似口径）
- fake 模式覆盖爆量台账/脉搏历史/异动/口径配置（e2e 可测）
```

- [ ] **Step 4: Commit + push + PR**

```bash
git add CHANGELOG.md
git commit -m "docs: changelog for panorama surge charts & pulse alerts

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push -u origin cc/pano-board-surge-pulse
gh pr create --title "feat(panorama): 爆量记录个股图表与触发标记 + 脉搏历史/异动推送" --body "$(cat <<'EOF'
## Summary
- 爆量记录 tab 点行出图，分时/5日标注每日首次爆量确认时刻（需求 1/2）
- 脉搏历史云端落盘 + 📈 浮层四分面小图；四类脉搏异动 → 页面提示条 + PushDeer（需求 3/4）
- 爆量口径 runtime_config 动态展示（需求 5 配套；检测范围全开走云端 .env，另行操作）
- 分时量柱 tick-rule 近似红绿上色（需求 6）

Spec: docs/superpowers/specs/2026-07-29-panorama-surge-pulse-design.md
Plan: docs/superpowers/plans/2026-07-29-panorama-surge-pulse.md

## Test
- 单测：pulse_watch 滑窗/冷却/seed、PulseSession 落盘与推送、loaders 降级、标记定位、notify 场景
- Playwright e2e（fake 8516）8 条 checklist 全过
EOF
)"
```

Expected: PR 创建成功，CI（Python 3.11/3.12）绿。

---

## 部署与收尾（PR 合并后，非编码任务）

1. CI 绿后 squash merge，tag `v0.28.0`（annotated，指向合并后 origin/main）。
2. 收盘后（15:10 后）`bash scripts/deploy-production.sh --target v0.28.0`。
3. 云端 82.156.0.68（lighthouse 用户）`/home/lighthouse/rquant/.env` 加 `RQUANT_SURGE_BOARDS=all` 并重启 `rquant-surge-watch.service`——**生产配置变更，执行前需用户确认**（设计 C 已获批，执行时再知会一声）。
4. `DEPLOY.md` 记录部署；次日盘中观察：pulse 文件生成、浮层出全天曲线、爆量记录页脚显示「主板/创业/科创/北交」、确认排队是否滞后（决定是否调 `tushare_rate_per_min`）。

## Self-Review 记录

- Spec 覆盖：设计 A→Task 4/5/6/7，设计 B→Task 1/2/3/4/8，设计 C→Task 3/7 + 部署步骤 3，设计 D→Task 5/6，测试→各 task + Task 9，非目标未混入。
- 占位符扫描：无 TBD/TODO；所有测试与实现均给出完整代码。
- 类型一致性：`PulsePoint/PulseAlert/PulseConfig/PulseSession` 签名在 Task 1/3/4/8 间一致；`load_surge_marks(ts_code, dates)` 与 `cached_surge_marks` 的 dates_key 编解码一致；`render_surge_log(snapshot)` 与 `render_body` 调用处同步；`volume_directions` 返回类别值与 UI 色彩映射键一致。
