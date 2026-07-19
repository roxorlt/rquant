# 分钟回补并发与可信 ETA Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 在不引入 DuckDB 并发写者的前提下并发拉取历史分钟数据，并支持可信 ETA、硬截止分段续跑和 `v0.25.2` Stage 1 恢复发布。

**Architecture:** 多个线程各自持有 Tushare adapter 和 SQLite claim 流程，所有 DuckDB
store 生命周期由同一把进程锁串行。CLI 用历史任务 P75/当前 EWMA 估算并发耗时，运行预算
只能缩短到下一保护窗口前，不能绕过 09:15-15:10。

**Tech Stack:** Python 3.11+、`concurrent.futures.ThreadPoolExecutor`、SQLite、DuckDB、
Pydantic、pytest、Bash、systemd transient service。

---

### Task 1: 并发网络拉取与 DuckDB 单写

**Files:**
- Modify: `tests/unit/test_backfill_runner.py`
- Modify: `src/rquant/intraday_backfill.py`

**Step 1:** 新增失败测试，用阻塞 adapter 证明两个请求必须同时进入，并记录 store factory
最大活跃数必须为 1。

**Step 2:** 运行：

```bash
PYTHONPATH=src /Users/roxor/brain/30-projects/rQuant/.venv/bin/python \
  -m pytest tests/unit/test_backfill_runner.py -q
```

预期新增测试因并发编排器不存在而失败。

**Step 3:** 新增有界 worker 编排器；每个 worker 调用既有 `run_backfill_manifest`，拥有独立
adapter，使用共享锁包装 store factory，最后聚合 `BackfillRunSummary`。

**Step 4:** 增加 worker 参数下界/上界、异常传播和聚合计数测试，运行同一测试文件直至通过。

### Task 2: 历史遥测 ETA 与分段截止

**Files:**
- Modify: `tests/unit/test_backfill_state.py`
- Modify: `tests/unit/test_backfill_cli.py`
- Modify: `src/rquant/backfill_state.py`
- Modify: `src/rquant/cli.py`

**Step 1:** 新增失败测试，证明历史耗时参考只读取成功、实际发过请求、同 source/freq 的最近
任务，并返回 P75。

**Step 2:** 新增失败测试，证明并发 ETA 取静态剩余估算和历史/当前遥测估算较大值，除以
worker 数后保留 25% 余量。

**Step 3:** 新增失败测试，证明 `--max-runtime-minutes` 的截止时间不会晚于下一交易日
09:05，保护窗口内无论参数如何都拒绝。

**Step 4:** 实现最小查询、估算和 CLI 参数；默认 8 workers、上限 16，同类成功任务不足
32 个时使用生产实测冷启动下限（10.416 秒/任务、651 行/秒），未提供运行预算时保留
现有全任务安全判断；尾部使用有效 worker 数，并保留单任务与 API 限频墙钟下界。

**Step 5:** 运行三个聚焦测试文件，确认旧串行默认和新并发路径都通过。

### Task 3: 可恢复 Stage 1 发布入口

**Files:**
- Move: `scripts/rollout-v0.25.1-stage1.sh` to `scripts/rollout-v0.25.2-stage1.sh`
- Move: `tests/unit/test_stage1_v0251_rollout_script.py` to `tests/unit/test_stage1_v0252_rollout_script.py`
- Modify: `CHANGELOG.md`
- Modify: `README.md`
- Modify: `pyproject.toml`
- Modify: `src/rquant/__init__.py`

**Step 1:** 先修改脚本契约测试，要求精确 `v0.25.2`、显式 resume manifest、8 workers、
运行预算、partial 时恢复原 timers 并输出 `ROLLOUT_RESULT=paused`，timer 只有逐个验证
active 后才算恢复。

**Step 2:** 运行脚本测试并确认因旧脚本缺少恢复入口而失败。

**Step 3:** 实现恢复入口。只有 resume manifest 状态 `completed` 才继续完整三策略链；
未完成时正常恢复 timers 并退出，不生成 snapshot 或正式回放。工作日 15:10 前即使
manifest 刚完成，也在软截止后由父进程硬监管、恢复 timers 后退出，完整链下个窗口继续。
所有长外部命令统一受下一 09:10 的 GNU `timeout` 硬截止约束，并在长步骤后重查窗口；
最终 recovery-only claim 被暂停时仍以相同 attempt 恢复。

**Step 4:** 更新版本和文档，运行 `bash -n`、脚本测试与 CLI/runner 聚焦测试。

### Task 4: 完整验证、PR 与精确发布

**Files:**
- Modify after production success: `DEPLOY.md`

**Step 1:** 运行 Ruff、`uv lock --check`、`git diff --check`、核心质量脚本和完整 pytest。

**Step 2:** 独立复审后提交、推送 PR；Python 3.11/3.12 CI 全绿后 squash merge，annotated
tag `v0.25.2` 必须精确指向合并后的 main SHA。

**Step 3:** 交易保护窗口外先 dry-run、再按精确 tag 部署。

**Step 4:** 以旧集合竞价 manifest、8 workers 和不晚于下一 09:05 的预算运行恢复脚本。
若 paused，保留状态并在下一安全窗口继续；不得在盘中续跑。

**Step 5:** manifest 完成后由同一脚本继续研究湖修复、三策略 snapshot、P0=0 审计、
formal smoke、备份、副本、preflight 和 timers 验收。

**Step 6:** 将精确任务数、实际吞吐、manifest/snapshot/binding/audit/result hash、备份和
回滚方式补入 `DEPLOY.md`，单独走部署记录 PR。
