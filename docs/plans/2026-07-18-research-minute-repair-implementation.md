# 历史分钟研究湖受控修复 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 新增一个以已完成回补 manifest 为范围真相、无需再次请求行情接口、支持
plan/apply 与批次原子回滚的历史分钟研究湖修复命令。

**Architecture:** 新模块从 SQLite 加载并校验不可变 `MinuteBackfillPlan`，比较生产只读
副本与研究湖的完整 1 分钟会话，按交易日合并缺失会话并复用研究湖内容寻址导出。
发布沿用全局研究锁、staged catalog、CAS journal 和 authority observation 链，但使用
独立 minute-repair journal，避免与每日增量或竞价修复耦合。

**Tech Stack:** Python 3.11+、Pydantic v2、pandas、DuckDB、SQLite、Parquet、pytest、
argparse。

---

### Task 1: 冻结 manifest 范围和分钟完整性计划

**Files:**
- Create: `src/rquant/research_minute_repair.py`
- Create: `tests/unit/test_research_minute_repair.py`

**Step 1: Write the failing tests**

覆盖未知 manifest、非 completed、持久化 task 篡改、空窗口、研究湖缺口推导、接受缺失
排除、生产会话不完整、错误 source/freq/date、重复主键和 241 分钟网格。

**Step 2: Run tests to verify RED**

```bash
.venv/bin/pytest tests/unit/test_research_minute_repair.py -q
```

Expected: FAIL because the module and plan models do not exist.

**Step 3: Implement the minimal plan models and loader**

实现冻结模型 `ResearchMinuteRepairDayPlan`、`ResearchMinuteRepairPlan`、
`ResearchMinuteRepairResult`；加载 `BackfillStateStore`，验证状态、payload、子任务，
并从持久化 windows 推导精确会话范围。

**Step 4: Run tests to verify GREEN**

运行同一测试文件并确认全部通过。

**Step 5: Commit**

```bash
git add src/rquant/research_minute_repair.py tests/unit/test_research_minute_repair.py
git commit -m "feat(research): plan governed minute lake repairs"
```

### Task 2: 合并并内容绑定多日分钟分区

**Files:**
- Modify: `src/rquant/research_minute_repair.py`
- Modify: `tests/unit/test_research_minute_repair.py`

**Step 1: Write the failing tests**

覆盖只读取缺失代码、保留既有研究行、相同业务值保留旧 `created_at`、新增行保留生产
`created_at`、输出物理主键唯一、排序稳定，以及源/合并业务内容变化会改变 plan ID。

**Step 2: Verify RED**

```bash
.venv/bin/pytest tests/unit/test_research_minute_repair.py -q
```

**Step 3: Implement merge and canonical hashes**

为每个受影响日期加载当前 manifest、读取生产会话、严格验证并合并；计划绑定旧 manifest、
会话键、源业务内容和合并后内容。

**Step 4: Verify GREEN**

运行同一测试文件。

**Step 5: Commit**

```bash
git add src/rquant/research_minute_repair.py tests/unit/test_research_minute_repair.py
git commit -m "feat(research): bind minute repairs to source content"
```

### Task 3: minute repair authority observation

**Files:**
- Modify: `src/rquant/research_ingest.py`
- Modify: `tests/unit/test_research_ingest.py`
- Modify: `tests/unit/test_research_minute_repair.py`

**Step 1: Write the failing tests**

覆盖 minute observation 模型前后 manifest 证据、解析联合类型、链路不倒退、lake binding、
稳定天数归零、下一次日增量从 1 开始和篡改 fail closed。

**Step 2: Verify RED**

```bash
.venv/bin/pytest \
  tests/unit/test_research_ingest.py \
  tests/unit/test_research_minute_repair.py -q
```

**Step 3: Implement the observation union**

新增 `ResearchMinuteRepairPartitionChange` 与 `ResearchMinuteRepairObservation`，扩展
observation parser、chain/lake binding 和 authority status。

**Step 4: Verify GREEN**

运行上述两组测试。

**Step 5: Commit**

```bash
git add src/rquant/research_ingest.py tests/unit/test_research_ingest.py \
  src/rquant/research_minute_repair.py tests/unit/test_research_minute_repair.py
git commit -m "feat(research): record minute repair authority observations"
```

### Task 4: 批次原子发布、回滚和崩溃恢复

**Files:**
- Modify: `src/rquant/research_minute_repair.py`
- Modify: `src/rquant/research_ingest.py`
- Modify: `tests/unit/test_research_minute_repair.py`
- Modify: `tests/unit/test_research_ingest.py`

**Step 1: Write the failing tests**

覆盖单次全局锁、多日 staged export、apply 时重建计划、stale `plan_id`、保护窗口、
immutable version 冲突、每个发布边界注入异常后的完整回滚、CAS 冲突不部分恢复、
interrupted minute journal 自动恢复。

**Step 2: Verify RED**

运行两组相关测试并确认因发布能力缺失而失败。

**Step 3: Implement atomic generation and journal**

复制主 catalog 和受影响 lake 目录，在 staging 中逐日导出，构建 readonly；写
minute-repair journal 后按 version、manifest、catalog、readonly、observation 顺序 CAS
发布，失败整体回滚。

**Step 4: Verify GREEN**

运行两组相关测试。

**Step 5: Commit**

```bash
git add src/rquant/research_minute_repair.py src/rquant/research_ingest.py \
  tests/unit/test_research_minute_repair.py tests/unit/test_research_ingest.py
git commit -m "feat(research): atomically publish minute lake repairs"
```

### Task 5: CLI、版本和中文运维文档

**Files:**
- Modify: `src/rquant/cli.py`
- Modify: `tests/unit/test_cli.py`
- Modify: `README.md`
- Modify: `CHANGELOG.md`
- Modify: `docs/deploy/research-daily-ingest-rollout.md`
- Modify: `src/rquant/__init__.py`
- Modify: `pyproject.toml`
- Modify: `uv.lock`

**Step 1: Write the failing CLI tests**

覆盖 parser、缺 plan ID、错误 SHA、开关关闭、plan 只读、apply JSON/退出码和异常顶层处理。

**Step 2: Verify RED**

```bash
.venv/bin/pytest tests/unit/test_cli.py -q
```

**Step 3: Implement CLI and docs**

新增 `research-repair-minute --manifest-id ... [--apply --plan-id ...]`，apply 要求
`RESEARCH_CLOUD_INGEST_ENABLED=true`。将功能版本提升为 `0.24.0`，补中文运行和回滚说明。

**Step 4: Verify GREEN**

运行 CLI 与所有 research repair 测试。

**Step 5: Commit**

```bash
git add src/rquant/cli.py tests/unit/test_cli.py README.md CHANGELOG.md \
  docs/deploy/research-daily-ingest-rollout.md src/rquant/__init__.py \
  pyproject.toml uv.lock
git commit -m "feat(research): expose governed minute repair workflow"
```

### Task 6: 全量验证、审查、PR 和精确版本部署

**Files:**
- Review all files changed by Tasks 1-5.

**Step 1: Run focused verification**

```bash
.venv/bin/pytest \
  tests/unit/test_research_minute_repair.py \
  tests/unit/test_research_repair.py \
  tests/unit/test_research_ingest.py \
  tests/unit/test_research_lake.py \
  tests/unit/test_cli.py -q
```

**Step 2: Run full verification**

```bash
.venv/bin/pytest -q
bash scripts/check-core-quality.sh
```

**Step 3: Independent review**

执行规格审查和代码质量审查，修复全部 P0/P1/P2 并重复验证。

**Step 4: Publish**

推送 feature 分支，CI 3.11/3.12 全绿后 squash merge；创建指向合并 SHA 的 annotated
`v0.24.0`，通过 `scripts/deploy-production.sh --target v0.24.0` 精确部署。

### Task 7: 生产修复与 Stage 1 继续验收

**Files:**
- Modify after successful rollout: `DEPLOY.md`

**Step 1: Backup and preview**

备份研究 catalog、只读 catalog、authority 和 manifests；运行 N-shape manifest 的
minute repair preview，核对 18,125 个会话、162 个日期、源完整性及 `plan_id`。

**Step 2: Apply**

在非保护窗口使用同一 `manifest_id + plan_id` apply。

**Step 3: Verify**

确认无 pending journal，主副 catalog 一致，authority 为 candidate/stable 0，研究湖
N-shape baseline >=95%、entry/exit >=99%，preflight、备份、服务和 timers 全绿。

**Step 4: Resume Stage 1**

依次完成 N-shape snapshot/fixed replay、growth manifest/snapshot/replay、auction
manifest/必要竞价修复/snapshot/replay。

**Step 5: Record**

更新 `DEPLOY.md`，走 docs PR，记录 tag/SHA、备份、manifest/plan/observation ID、覆盖结果、
回放摘要和回滚命令。
