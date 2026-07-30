# 爆量 Push 五交易日次数 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 同一标的跨交易日再次满足爆量条件时继续推送，并在每只标的的报文中展示包含本次的“近5日推送次数 n”。

**Architecture:** surge-watch 启动时从只读交易日历取得今日及前四个交易日，只扫描这些日期的事件 JSONL。历史记录按“代码 + 交易日”去重形成 `SurgePushHistory`；只有今日代码恢复到 `pushed_today`，过去交易日仅参与计数，因此跨日不抑制、同日重启不重复。确认事件在入队时写入 `push_count_5d`，报文和事件 JSONL 使用同一固化值。

**Tech Stack:** Python 3.11+、Pydantic、DuckDB 只读副本、JSONL、pytest。

---

### Task 1: 固化历史窗口与跨日语义

**Files:**
- Modify: `tests/unit/test_surge_watch.py`
- Modify: `src/rquant/surge_watch.py`

**Step 1: Write the failing tests**

- 构造包含周末和窗口外文件的事件目录，断言只统计传入的五个交易日。
- 同一代码同日重复记录按一天计数。
- 历史前一交易日出现的代码不进入今日去重集合；今日已有记录进入今日去重集合。
- 损坏行跳过，不阻断其余历史恢复。

**Step 2: Run tests to verify RED**

Run: `pytest tests/unit/test_surge_watch.py -q`

Expected: FAIL，因为 `SurgePushHistory` 和 `load_recent_push_history` 尚不存在。

**Step 3: Implement the minimal history loader**

- 新增 Pydantic `SurgePushHistory`，保存精确窗口日期和每个代码出现过的交易日集合。
- 新增只读交易日历查询，失败时退化为仅今日，保证推送主流程继续运行。
- 新增 JSONL 历史加载器，只打开精确窗口文件，按代码/日期去重。

**Step 4: Run tests to verify GREEN**

Run: `pytest tests/unit/test_surge_watch.py -q`

Expected: 新历史窗口用例通过。

### Task 2: 把计数接入确认状态机和手机报文

**Files:**
- Modify: `tests/unit/test_surge_watch.py`
- Modify: `src/rquant/surge_watch.py`

**Step 1: Write the failing tests**

- 前一交易日已推的代码今日仍能确认，`push_count_5d == 2`。
- 今日事件已存在时 watcher 不再次确认。
- 手机报文逐票块包含 `近5日推送次数：2`。

**Step 2: Run tests to verify RED**

Expected: FAIL，因为确认模型和渲染器尚未接入计数。

**Step 3: Implement minimal wiring**

- `SurgeConfirmed` 新增最小值为 1 的 `push_count_5d`。
- `SurgeWatcher` 接收历史，在确认时合并今日日期并计算不同交易日数量。
- `build_surge_messages` 在逐票信息块单独增加一行次数。
- `run_surge_watch` 启动时加载历史并传给 watcher。

**Step 4: Run tests to verify GREEN**

Run: `pytest tests/unit/test_surge_watch.py -q`

Expected: 全部通过。

### Task 3: 文档与完整验证

**Files:**
- Modify: `CHANGELOG.md`
- Modify: `src/rquant/__init__.py`
- Modify: `pyproject.toml`

**Step 1:** 在 Unreleased 记录跨日重触发、同日重启去重和五交易日计数口径。

**Step 2:** 按 patch 版本更新版本号。

**Step 3:** 运行定向测试、通知测试和完整测试套件。

**Step 4:** 用最近一条云端真实事件做 dry-run 报文核验，禁止真实 Push。
