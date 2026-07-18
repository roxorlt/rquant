# 集合竞价历史缺口受控修复实施计划

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 提供一个 plan/apply 双阶段、批次原子、可崩溃恢复的集合竞价历史修复命令，
并用它修复七个生产研究湖缺口后完成三策略固定回放验收。

**Architecture:** 新增独立的 `research_repair` 领域模块负责严格质量检查、规范计划哈希、
分区合并和批次发布；复用研究湖 immutable export、catalog、全局发布锁和权威观察链，
但不复用要求分钟/竞价连续追加的每日 ingest 事务。当前权威标记升级为日增量观察与
历史修复观察的可判别联合结构，修复后稳定天数归零。

**Tech Stack:** Python 3.11+、Pydantic v2、pandas、DuckDB、Parquet、pytest、argparse。

---

## Task 1: 严格竞价审计和规范计划

**Files:**
- Create: `src/rquant/research_repair.py`
- Create: `tests/unit/test_research_repair.py`

1. 写失败测试，覆盖目标日期排序去重、缺字段、错误日期、错误来源/类型、重复物理主键、
   非正价格/成交量、两侧 98% 整数边界、日线全市场不完整、规范哈希稳定性。
2. 运行：

   ```bash
   uv run pytest tests/unit/test_research_repair.py -q
   ```

   确认因模块或行为缺失而失败。
3. 实现冻结 Pydantic 模型：
   `ResearchAuctionRepairDayPlan`、`ResearchAuctionRepairPlan`、
   `ResearchAuctionRepairResult` 和严格审计函数。
4. 计划必须绑定代码、权威/catalog/readonly/manifest 基线、预期 universe、实际 Tushare
   内容和合并后内容。
5. 再次运行同一测试，确认通过。

## Task 2: 构建修复分区且不篡改历史时间

**Files:**
- Modify: `src/rquant/research_repair.py`
- Modify: `tests/unit/test_research_repair.py`

1. 写失败测试，覆盖：
   - 保留 fallback 和其他来源。
   - 替换 Tushare `open_realtime` 行。
   - 相同业务值保留原 `created_at`。
   - 新增/变更行使用真实修复时间。
   - 输出物理主键唯一、排序稳定。
   - 完全相同批次返回 `unchanged`。
2. 运行单测确认红灯。
3. 复用研究湖 schema 和内容哈希语义实现合并与单日 export source。
4. 运行单测确认绿灯。

## Task 3: 批次原子发布、回滚和恢复

**Files:**
- Modify: `src/rquant/research_repair.py`
- Modify: `src/rquant/research_ingest.py`
- Modify: `tests/unit/test_research_repair.py`
- Modify: `tests/unit/test_research_ingest.py`

1. 写失败测试，覆盖：
   - 多日期只使用一次全局发布锁。
   - 任意日期获取/质量失败时零文件变化。
   - 计划后 catalog/manifest/current 变化令 apply 失败。
   - 发布中每个边界注入异常后完整恢复。
   - 恢复前 CAS 冲突时不做部分回滚。
   - 新 immutable 版本只有在回滚确认后删除。
2. 实现修复专用 generation baseline 和 schema v1 批次 journal。
3. staged catalog 中逐日期导出竞价分区并构建 readonly catalog。
4. 在锁内完成版本、manifest、catalog、readonly、observation/current 的 CAS 发布。
5. 将通用研究事务恢复入口扩展为可识别 daily 与 auction-repair journal。
6. 运行相关测试确认绿灯。

## Task 4: 修复观察与稳定性重置

**Files:**
- Modify: `src/rquant/research_ingest.py`
- Modify: `src/rquant/research_repair.py`
- Modify: `tests/unit/test_research_ingest.py`
- Modify: `tests/unit/test_research_repair.py`
- Modify: `tests/unit/test_research_snapshot.py`

1. 写失败测试，覆盖：
   - 修复观察继承 bootstrap lineage 且 latest trade date 不倒退。
   - 当前 authority 可解析两种观察。
   - 修复后 `stable_trading_days=0` 且不可提升。
   - 下一次正常 candidate 从 1 开始。
   - 旧内容寻址 manifest 仍可通过快照验证。
   - 篡改修复观察或当前分区会 fail closed。
2. 新增 `ResearchAuctionRepairObservation`，用可判别联合类型读取当前标记和观察索引。
3. 调整 authority/lake binding 检查和每日 ingest 的稳定性计算。
4. 运行三组测试确认绿灯。

## Task 5: CLI、安全门和用户文档

**Files:**
- Modify: `src/rquant/cli.py`
- Modify: `tests/unit/test_cli.py`
- Modify: `README.md`
- Modify: `CHANGELOG.md`
- Modify: `src/rquant/__init__.py`
- Modify: `pyproject.toml`

1. 写 CLI 失败测试，覆盖重复 `--date`、缺 apply plan、错误 SHA、开关关闭、保护窗口、
   plan 不写文件、apply 退出码和 JSON 输出。
2. 实现 `research-repair-auction` parser/dispatch/handler。
3. plan 阶段真实请求接口但不持久化；apply 阶段要求开关、干净提交和 `--plan-id`。
4. 更新中文使用说明、Changelog 与版本为 `0.23.0`。
5. 运行：

   ```bash
   uv run pytest \
     tests/unit/test_research_repair.py \
     tests/unit/test_research_ingest.py \
     tests/unit/test_research_snapshot.py \
     tests/unit/test_cli.py -q
   ```

## Task 6: 本地完整验证与发布

1. 运行格式、静态和项目核心门：

   ```bash
   uv run ruff check src/rquant/research_repair.py src/rquant/research_ingest.py src/rquant/cli.py tests/unit/test_research_repair.py
   uv run pytest tests/unit -q
   uv run pytest -q
   ```
2. 运行独立代码审查，修复所有 P0/P1/P2。
3. 检查 git diff 仅包含本功能，提交 feature 分支并推送。
4. 创建 PR，等待 Python 3.11/3.12 CI 全绿后 squash merge。
5. 创建 annotated `v0.23.0`，确认 tag 精确指向合并后的 `origin/main`。

## Task 7: 生产七日修复

1. 周末或工作日 15:10 后只读核对现状。
2. 备份研究 catalog、只读 catalog、authority 状态和 lake manifests。
3. 通过 `scripts/deploy-production.sh --target v0.23.0 --dry-run` 预演，再部署精确 tag。
4. 执行七日期 plan，保存 JSON 与 `plan_id`。
5. 人工/程序核对每一天预期 universe、Tushare 有效覆盖、精确率和变更统计。
6. 使用同一日期集与 `plan_id` apply。
7. 验收：
   - 七个目标 manifest/catalog 指向一致。
   - immutable 文件哈希和逻辑内容哈希正确。
   - 主副 catalog 摘要一致。
   - authority 为 candidate、稳定天数 0、不可提升。
   - 无 pending journal。
   - preflight 全绿、备份和 timers 正常。

## Task 8: 三策略固定回放与部署记录

1. 重建并 apply `n_shape`、`growth_board_surge`、`auction_gap` 三个 manifest/snapshot。
2. 对固定日期范围执行三策略回放，记录候选数、交易数、胜率、平均/中位收益、最大回撤、
   缺失原因和 PIT 违规计数。
3. 明确报告样本量与数据覆盖，不用单个交易日命中率替代整体结论。
4. 更新 `DEPLOY.md`：日期、tag、SHA、备份、plan ID、七日质量统计、权威状态、回放摘要、
   回滚命令。
5. 通过 docs PR 合并部署记录，清理临时 worktree、远端 `/tmp` 脚本；保留审计日志和
   约定期限内的失败证据备份。
