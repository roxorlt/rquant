# 阶段 1：数据契约、PIT 状态与回补规划实施计划

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**目标：** 建立可版本化的数据契约、交易日/PIT 可见性、可重复的数据质量审计和可断点续跑的策略回补清单，使每次研究都能复现资格分母、数据覆盖率与缺失原因。

**架构：** DuckDB 保存研究事实、快照、覆盖率和质量问题；`schema_migration` 作为本机迁移账本，不参与云端/本地数据合并。长时间回补的任务状态放 SQLite，避免回补写者持有 DuckDB 锁时 `backfill-status` 无法查询。所有策略通过 typed registry 声明价格口径、最早可见时间、新鲜度和回补窗口。

**技术栈：** Python 3.11+、Pydantic、DuckDB、SQLite、pandas、argparse、pytest、ruff。

**执行状态（2026-07-14）：** PR-A 已随 v0.14.0 部署；PR-B Tasks 6-11 已完成实现和审查，
全量 1682 项测试通过，待合并为 v0.15.0 并按受控顺序完成云端/本地交易日历 bootstrap；
PR-C、PR-D 尚未完成。

---

## 交付批次

阶段 1 分四个可独立合并的 PR，依赖顺序固定：

1. **PR-A：迁移内核、元数据表、数据契约与交易日历**
2. **PR-B：历史证券状态、质量审计、复权与 PIT 守卫**
3. **PR-C：策略资格全集、回补 manifest、SQLite 状态与四条 CLI**
4. **PR-D：preflight、真实数据审计、覆盖门和阶段验收**

每个 PR 都必须从最新 `origin/main` 建隔离 worktree，按 TDD 完成，CI 全绿后再合并。阶段 1
全部验收前不进入阶段 2，也不运行大规模真实回补。

## PR-A：迁移内核、元数据表、数据契约与交易日历

### Task 1：版本化 schema migration

**Files:**
- Create: `src/rquant/storage/migrations.py`
- Modify: `src/rquant/storage/schema.py`
- Modify: `src/rquant/storage/duckdb.py`
- Modify: `src/rquant/research_sync.py`
- Create: `tests/unit/test_schema_migrations.py`
- Modify: `tests/unit/test_research_sync.py`

**Step 1: 写失败测试**

覆盖以下行为：

- fresh DB 创建 `schema_migration` 并记录已应用版本/checksum；
- legacy DB 可升级，二次打开不重复执行；
- migration 中途失败时 DDL 与账本同事务回滚；
- 同一 version 的 checksum 改变时拒绝启动；
- `DuckDBStore` 与 `research_sync` 使用同一 `initialize_schema()`；
- `schema_migration` 属于 `LOCAL_ONLY_TABLES`，不会从云端备份导入。

**Step 2: 验证 RED**

Run:

```bash
uv run pytest -q tests/unit/test_schema_migrations.py tests/unit/test_research_sync.py
```

Expected: FAIL，原因是 migration registry、账本和 `LOCAL_ONLY_TABLES` 尚不存在。

**Step 3: 最小实现**

- `Migration` 使用 frozen Pydantic model，字段为 `version`、`name`、`statements`、`checksum`；
- `schema_migration` 先 bootstrap，再在单事务内执行未应用 migration 并写账本；
- `BASE_DDL` 只包含建表，旧 `ALTER ... IF NOT EXISTS` 迁入有版本的 registry；
- `DuckDBStore._init_schema()` 与 `research_sync` 都调用共享函数；
- 全表同步分类变为 `REPLACE_TABLES | MERGE_TABLES | LOCAL_ONLY_TABLES`，三者互斥且覆盖所有表。

**Step 4: 验证 GREEN**

运行 Task 1 测试、`tests/unit/test_storage_duckdb.py` 和 `tests/unit/test_signal_provenance.py`。

**Step 5: Commit**

```bash
git commit -m "feat(storage): add versioned schema migrations"
```

### Task 2：研究数据元数据表与 typed storage API

**Files:**
- Modify: `src/rquant/storage/schema.py`
- Create: `src/rquant/data_metadata.py`
- Modify: `src/rquant/storage/duckdb.py`
- Modify: `src/rquant/research_sync.py`
- Create: `tests/unit/test_data_metadata.py`

**Step 1: 写失败测试**

覆盖：

- `dataset_snapshot(snapshot_id)` 创建、完成和 JSON watermark 往返；
- `dataset_coverage(snapshot_id, dataset_id, coverage_scope)` 幂等 upsert；
- coverage 的分子不能大于分母，空分母只能得到 `None`，不能伪造 100%；
- `data_quality_issue(issue_id)` 重复扫描更新 `last_seen_at`，resolve 后可再次 reopen；
- 三张业务表进入 `MERGE_TABLES`，migration ledger 保持 local-only。

**Step 2: 验证 RED**

Run: `uv run pytest -q tests/unit/test_data_metadata.py`

Expected: FAIL，原因是模型、DDL 与 storage API 不存在。

**Step 3: 最小实现**

- 所有跨层模型使用 Pydantic，不传裸 dict；
- 稳定 ID 由规范化字段 SHA-256 生成；
- 时间统一写 UTC ISO 8601；
- 不建立 DuckDB FK，避免云端/本地同步顺序耦合；
- `dataset_id` 与物理 `table_name` 分开保存。

**Step 4: 验证 GREEN**

运行新测试、`test_research_manifest.py` 和 `test_research_sync.py`。

**Step 5: Commit**

```bash
git commit -m "feat(data): add dataset metadata and quality issue storage"
```

### Task 3：数据契约 registry

**Files:**
- Create: `src/rquant/data_contracts.py`
- Create: `tests/unit/test_data_contracts.py`
- Modify: `src/rquant/dataset_backfill.py`

**Step 1: 写失败测试**

定义并验证：

- `DatasetContract`、`PriceBasis`、`VisibilityRule`、`FreshnessRule`；
- dataset id 唯一，物理表存在，日期/时间列明确；
- `daily_bar`、`minute_bar`、`auction_bar`、`adj_factor`、`limit_list_daily`、资金流和板块首批契约；
- 盘后日级字段在同日盘中不可用，分钟字段只到 `as_of_time`，竞价在 09:25 后可用；
- registry 与 `dataset_backfill.DATASETS` 的逻辑 id/物理表映射一致。

**Step 2: 验证 RED**

Run: `uv run pytest -q tests/unit/test_data_contracts.py`

Expected: FAIL，原因是 registry 不存在。

**Step 3: 最小实现**

先只声明事实与校验，不在本任务改策略查询。缺少明确历史可见时间的接口标为
`PANEL_CLOSE_NEXT_SESSION` 或 `UNKNOWN`，不得默认盘中可用。

**Step 4: 验证 GREEN**

运行新测试与 `tests/unit/test_dataset_backfill.py`。

**Step 5: Commit**

```bash
git commit -m "feat(data): register point-in-time dataset contracts"
```

### Task 4：持久化交易日历

**Files:**
- Modify: `src/rquant/storage/schema.py`
- Modify: `src/rquant/storage/duckdb.py`
- Create: `src/rquant/trade_calendar.py`
- Modify: `src/rquant/adapter/tushare.py`
- Create: `tests/unit/test_trade_calendar.py`

**Step 1: 写失败测试**

覆盖周末、法定节假日、交易日、最近/下一交易日、日历缺口和幂等 upsert。查询日历不得用
`daily_bar` 猜测，因为停牌或数据缺失不等于休市。

**Step 2: 验证 RED**

Run: `uv run pytest -q tests/unit/test_trade_calendar.py`

Expected: FAIL，原因是 `trade_calendar` 不存在。

**Step 3: 最小实现**

表主键为 `(exchange, cal_date)`，记录 `is_open`、`pretrade_date`、`source`、`updated_at`。
Tushare adapter 暴露原始日历 DataFrame；现有返回 `list[date]` 的便捷入口保持兼容。

**Step 4: 验证 GREEN**

运行新测试、`test_market_backfill.py` 和 `test_dataset_backfill.py`。

**Step 5: Commit**

```bash
git commit -m "feat(data): persist authoritative trade calendar"
```

### Task 5：PR-A 收口

**Files:**
- Modify: `CHANGELOG.md`
- Modify: `README.md`
- Modify: `docs/plans/2026-07-13-rquant-trustworthy-strategy-roadmap.md`

运行：

```bash
bash scripts/check-core-quality.sh
uv run pytest -q
```

验收 fresh/legacy DB、迁移回滚、同步分类、契约 registry 和交易日历；通过 spec review 与 code
quality review 后创建 PR-A。PR-A 不修改生产数据，不运行历史清理。

## PR-B：历史状态、质量审计、复权与 PIT

### Task 6：质量审计框架

**Files:**
- Create: `src/rquant/data_quality.py`
- Create: `tests/unit/test_data_quality.py`

实现 typed `AuditRule` / `AuditFinding` / `AuditReport`，稳定 issue id、严重度、P0 阻断标记、
重复扫描幂等和 resolve/reopen。审计默认只读；修复必须独立命令、默认 dry-run，并记录 before/
after 计数。

### Task 7：历史证券名称/ST 状态

Task 7 按顺序拆成三个子任务，避免存储、派生和策略旁路同时改动：

#### Task 7A：历史状态事实表、回补与覆盖审计

**Files:**
- Create: `src/rquant/security_status.py`
- Modify: `src/rquant/storage/schema.py`
- Modify: `src/rquant/storage/migrations.py`
- Modify: `src/rquant/storage/duckdb.py`
- Modify: `src/rquant/adapter/tushare.py`
- Modify: `src/rquant/data_quality.py`
- Modify: `src/rquant/data_contracts.py`
- Modify: `src/rquant/research_sync.py`
- Create: `tests/unit/test_historical_security_status.py`

新增 `stock_status_daily(ts_code, trade_date)`，名称和 `is_st` 均允许 NULL，显式表达 unknown；
记录来源、`available_at` 和写入时间。历史主事实使用 Tushare `namechange` 的有效名称区间，按
日期窗口分片、规范化去重后物化到 `daily_bar` 的实际资格键。`stock_st` 只用于确认 ST 正例和
交叉核验：空返回或未命中绝不能证明非 ST；与历史名称冲突时保守标 unknown/P0。`st` 接口当前
账号无权限且首版不需要。

回补前或存在区间空洞、冲突时，质量规则必须产生 P0 issue。单次接口结果达到行数上限时按不
完整失败，不能把截断结果当全量。`stock_basic.name` 只代表当前快照，禁止用于历史兜底。

#### Task 7B：日线派生与筛选层采用按日状态

**Files:**
- Modify: `src/rquant/state/derive.py`
- Modify: `src/rquant/state/__init__.py`
- Modify: `src/rquant/ingest.py`
- Modify: `src/rquant/market_backfill.py`
- Modify: `src/rquant/security_status.py`
- Modify: `src/rquant/storage/duckdb.py`
- Modify: `src/rquant/cli.py`
- Modify: `src/rquant/screen/loader.py`
- Modify: `src/rquant/screen/rules.py`
- Modify: `src/rquant/market_context.py`
- Modify: `scripts/ingest_daily.py`
- Modify: `scripts/backfill_market.py`
- Modify: `tests/unit/test_state.py`
- Create: `tests/unit/test_ingest.py`
- Modify: `tests/unit/test_market_backfill.py`
- Modify: `tests/unit/test_screen_loader.py`
- Modify: `tests/unit/test_screen_rules.py`
- Create: `tests/unit/test_ingest_scripts.py`
- Modify: `tests/unit/test_historical_security_status.py`
- Modify: `tests/unit/test_cli.py`
- Modify: `tests/integration/test_screen_e2e.py`

先构造“戴帽前/戴帽期/摘帽后/未知”失败测试。`derive_state()` 必须消费逐日 nullable 状态，
且只有 `available_at <=` 对应派生或回放的决策时点才可见；只按交易日 join 仍属于 PIT 泄漏。
unknown 时涨跌停幅度、价格和涨停/首板/一字板/连板派生标记不可用，并中断跨日连板链；
`load_universe()` 按 `(ts_code, trade_date)` 取历史名称/ST，缺行、冲突和未来才可见均保留 NULL，
不得回退 `daily_state` 或当前 `stock_basic`。`not_st()` 与所有否定型/“窗口内无……”规则只允许
事实明确时通过；窗口聚合和市场情绪也必须携带完整性，不得让 SQL 聚合忽略 NULL 后伪造零值。
重算历史状态不得再读取当前 `stock_basic.name`。

日常 ingest 的远端请求必须在 writer 打开前完成，状态/行情/派生写入同事务；历史日期更正要更新
受影响的状态与指标未来尾部。全市场历史回补按日短写入并先失效状态尾部，全部日期结束后只重算
一次，不能在每个日期重复扫描多年历史。涨跌停幅度按历史制度日期计算：创业板 2020-08-24
前后及风险警示比例必须区分，科创板/北交所风险警示不得误套主板 5%。IPO、重新上市和其他无
涨跌幅限制窗口在取得权威逐日资格事实前标为不支持，正式研究必须排除，不能从第一根已有 K 线
猜测。

#### Task 7C：清除策略专用的当前名称旁路

**Files:**
- Modify: `src/rquant/auction_gap_strategy.py`
- Modify: `src/rquant/growth_board_surge_strategy.py`
- Modify: `src/rquant/dashboard/strategy_lab.py`
- Modify: corresponding unit tests

竞价回放、科创/创业放量候选和 Lab 候选计数都必须按历史交易日 join
`stock_status_daily`，删除当前名称推断 ST 的 fallback。普通筛选、策略回放和统计分母使用同一
状态来源后，Task 7 才算完成。

### Task 7.5：权威交易日历 bootstrap

**Files:**
- Modify: `src/rquant/cli.py`
- Modify: `src/rquant/trade_calendar.py`
- Modify: `scripts/sync-from-cloud.sh`
- Modify: `tests/unit/test_trade_calendar.py`
- Modify: `tests/unit/test_cli.py`

PR-A 只创建了 `trade_calendar`，生产表当前仍为空。增加幂等的日历回补入口，先在云端覆盖研究
区间并同步到本地，再启用 Task 8 的 fail-closed 守卫。同步脚本增加显式
`--skip-post-sync-captures`，避免日历尚未核验就自动 capture。部署顺序必须是“暂停相关采集 ->
部署代码 -> 回补并核验云端主库/副本日历 -> 同步并核验本地主库/副本 -> 手工试跑 capture ->
恢复调度”，否则所有日期都会因日历 unknown 被拒绝。

### Task 8：涨停池交易日守卫与修复计划

**Files:**
- Modify: `src/rquant/limit_up_pool.py`
- Modify: `src/rquant/data_quality.py`
- Modify: `src/rquant/storage/schema.py`
- Modify: `src/rquant/storage/migrations.py`
- Modify: `src/rquant/storage/duckdb.py`
- Modify: `src/rquant/research_sync.py`
- Modify: `src/rquant/cli.py`
- Create: `tests/unit/test_limit_up_pool_calendar_guard.py`

capture 在远端抓取前和最终写入前各检查一次权威日历：已知休市日拒写并记录 issue；日历
unknown 必须 fail closed、产生 P0 并返回非零，不能按休市日静默删除。修复命令先生成带排序
候选主键、before count 和稳定 plan id 的 dry-run 报告；apply 必须同时显式传入 `--apply` 和
dry-run 的 plan id，并在单事务内重算候选集做 CAS，集合变化则零删除。只允许删除日历明确为
休市日的行；unknown 先补日历。删除、after count 和持久化 repair audit 同事务提交，不能直接
在部署脚本里清生产数据。

### Task 9：日线/分钟一致性审计

**Files:**
- Modify: `src/rquant/data_quality.py`
- Create: `tests/unit/test_daily_minute_consistency.py`

区分“有分钟无日线”“资格日无分钟”“缺时段”“跨源重复”“停牌/未上市允许缺失”。分钟根数
按数据源和时间戳语义配置，不把 240/241 的差异硬编码成全局真理。

### Task 10：PIT 复权价格契约

**Files:**
- Modify: `src/rquant/price_adjustment.py`
- Modify: `src/rquant/storage/duckdb.py`
- Modify: `src/rquant/stock_features.py`
- Modify: `src/rquant/volume_profile.py`
- Create: `tests/unit/test_price_basis_pit.py`

`get_daily_qfq(end=T)` 只能用 `<=T` 的因子作锚；缺因子返回 unavailable，不得 `fillna(1.0)`
伪装同口径。涨跌停继续用原始 `pre_close`。

### Task 11：available_at 查询守卫

**Files:**
- Create: `src/rquant/pit_visibility.py`
- Create: `tests/unit/test_pit_visibility.py`

实现纯函数与受控查询入口：同日盘后资金流在盘中不可见、T-1 可见、竞价 09:25 后可见、
分钟只到当前时刻、派生字段的 `available_at` 为输入最大值。先不重写全部策略，PR-C 的资格解析
器必须使用该入口。

## PR-C：资格全集与可断点回补 manifest

### Task 12：策略回补规格与资格全集

**Files:**
- Create: `src/rquant/backfill_manifest.py`
- Modify: `src/rquant/growth_board_surge_strategy.py`
- Modify: `src/rquant/presets.py`
- Create: `tests/unit/test_backfill_manifest.py`

实现 `StrategyBackfillSpec`、`EligibilityRecord` 和窗口需求。科创/创业资格查询必须在读取分钟表
前完成；N 字按日重建，不依赖历史 `screen_result`；竞价策略标记为 `daily+auction`。

### Task 13：窗口合并、覆盖计算与 ETA

**Files:**
- Modify: `src/rquant/backfill_manifest.py`
- Create: `tests/unit/test_backfill_planner.py`

按交易日展开 90 日基准、B 日和最多 10 日退出窗口；同股票重叠窗口合并，再按 8000 行上限
切块。只生成缺失任务，ETA 同时展示请求数、预计行数、磁盘、限频时间和置信度。

### Task 14：SQLite 回补状态

**Files:**
- Create: `src/rquant/backfill_state.py`
- Create: `tests/unit/test_backfill_state.py`

实现 manifest/task/eligibility 持久化、原子 claim、崩溃 running 恢复、重试上限、失败原因、
EWMA ETA。状态库使用配置中的独立 SQLite 路径，不放 DuckDB。

### Task 15：回补执行器

**Files:**
- Modify: `src/rquant/intraday_backfill.py`
- Modify: `src/rquant/adapter/tushare.py`
- Create: `tests/unit/test_backfill_runner.py`

分钟接口复用统一退避/限频；成功任务不再请求；空返回必须分类为允许缺失或 `source_empty`；
中断后只领取 pending/可重试 failed。写入后更新真实请求、行数、耗时和覆盖率。

### Task 16：四条 CLI

**Files:**
- Modify: `src/rquant/cli.py`
- Modify: `tests/unit/test_cli.py`

实现：

```text
rquant backfill-plan --strategy ... --start-date ... --end-date ...
rquant backfill-run --manifest-id ... [--retry-failed]
rquant backfill-status --manifest-id ... [--json]
rquant dataset-snapshot --strategy ... --as-of ... --manifest-id ...
```

未知 manifest、失败任务和覆盖未达标返回非零 exit code。`dataset-snapshot` 文案明确不是
`DatasetSpec.mode=snapshot` 的整表刷新。

## PR-D：preflight、真实数据审计与阶段验收

### Task 17：契约驱动 preflight freshness

**Files:**
- Modify: `src/rquant/preflight.py`
- Modify: `tests/unit/test_preflight.py`
- Create: `tests/unit/test_preflight_freshness.py`

支持 `trade_date`、`trade_time`、无日期静态表、交易日阈值、必需空表、只读副本年龄。覆盖
`minute_bar`、`auction_bar`、`adj_factor`、`limit_list_daily`、资金流和板块关键表。

### Task 18：data-audit 与正式研究阻断

**Files:**
- Modify: `src/rquant/cli.py`
- Modify: `src/rquant/dashboard/strategy_lab.py`
- Create: `tests/unit/test_data_audit_cli.py`
- Create: `tests/unit/test_strategy_research_gate.py`

实现 `rquant data-audit --as-of YYYY-MM-DD`；存在 P0 issue 或资格/B-S 覆盖低于 99%、历史基准
低于 95% 时，Lab 禁止“正式回测”，但允许 exploratory dry-run，并显示具体缺失原因。

### Task 19：真实数据只读核验与回补计划

对本地只读副本运行审计，从最新日期向旧查找人工样本，记录回退天数。生成但不执行 P0/P1/P2
回补 manifest，输出请求数、预计行数、磁盘和 ETA；大规模下载必须另行确认。

### Task 20：阶段 1 验收

**Files:**
- Modify: `CHANGELOG.md`
- Modify: `README.md`
- Modify: `docs/plans/2026-07-13-rquant-trustworthy-strategy-roadmap.md`
- Create: `docs/analysis/2026-07-XX-stage1-data-contract-acceptance.md`

验收条件：

- 资格全集分母可复现，不从已有分钟数据反推；
- B/S 窗口覆盖门 99%，历史基准门 95%，缺失不可静默为零；
- 周末污染有可审计修复计划，历史 ST 状态可按日查询；
- PIT、复权、freshness 和只读副本测试通过；
- 回补 manifest 可断点续跑并能给出可信 ETA；
- 全量 pytest、核心 ruff、CI 3.11/3.12 全绿；
- 阶段 1 的四个 PR 均合并并按受控发布纪律部署。

通过后再创建阶段 2 的逐文件实施计划。
