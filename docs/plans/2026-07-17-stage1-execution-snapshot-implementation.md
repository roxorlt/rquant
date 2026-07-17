# Stage 1 不可变执行快照实施计划

> **执行要求：** 使用 TDD，逐任务完成；每个任务先观察 RED，再写最小实现并运行目标测试。

**目标：** 让三类现有策略的正式研究绑定不可变数据、独立覆盖证据和同一执行会话，关闭
`snapshot_execution_unbound`，并完成真实策略级覆盖验收。

**架构：** 大表绑定研究湖内容寻址 Parquet，小表物化为策略区间执行包；DuckDB
`dataset_snapshot_binding` 保存一对一绑定；formal gate 和计算共享
`ResearchExecutionSession`。

**技术栈：** Python 3.11+、Pydantic、DuckDB、Parquet、SQLite、pytest、ruff。

**实施状态（2026-07-17）：** Tasks 1-8 与本地端到端复现已完成。`dataset-snapshot`
采用同一命令的默认预演/显式 `--apply` 两阶段语义，并在 apply 成功时同步发布 binding；没有新增
容易混淆的第二个子命令。剩余 Task 9 生产验收必须在合并部署后，按三类策略逐个执行，结果
不得由 fixture 或旧回测替代。

---

## Task 1：执行绑定模型与 migration v10

**Files:**
- Modify: `src/rquant/data_metadata.py`
- Modify: `src/rquant/storage/schema.py`
- Modify: `src/rquant/storage/migrations.py`
- Modify: `src/rquant/storage/duckdb.py`
- Modify: `src/rquant/research_sync.py`
- Modify: `tests/unit/test_data_metadata.py`
- Modify: `tests/unit/test_schema_migrations.py`
- Modify: `tests/unit/test_research_sync.py`

**RED tests:**

- `DatasetSnapshotBindingManifest` 的规范化 hash 与 artifact 顺序稳定；
- 绝对根路径不参与 binding hash；
- binding 只能引用已存在 ready snapshot；
- `building -> ready` 支持完全相同的幂等重试；
- ready binding 不可改写；
- manifest hash、binding hash、snapshot ID 不一致时拒绝写入；
- migration v10 在 fresh/legacy DB 幂等应用；
- binding 表进入 metadata 同步集合，migration ledger 保持 local-only。

**Minimal implementation:**

- 增加 frozen Pydantic models：artifact、manifest、binding、finalization；
- 新增 `DATASET_SNAPSHOT_BINDING_DDL` 和 append-only migration v10；
- storage 增加 begin/finalize/get binding API 和 CAS；
- 保留现有 snapshot ID 算法不变。

**Verify:**

```bash
uv run pytest -q \
  tests/unit/test_data_metadata.py \
  tests/unit/test_schema_migrations.py \
  tests/unit/test_research_sync.py
```

## Task 2：研究湖精确版本解析与 artifact 验证

**Files:**
- Modify: `src/rquant/research_catalog.py`
- Modify: `src/rquant/research_lake.py`
- Create: `src/rquant/research_snapshot.py`
- Create: `tests/unit/test_research_snapshot.py`
- Modify: `tests/unit/test_research_lake.py`
- Modify: `tests/unit/test_research_catalog.py`

**RED tests:**

- 按 dataset/date/freq 返回 catalog 当前精确 partition record；
- resolver 返回 `versions/<file_hash>.parquet`，不返回可变 manifest head；
- 文件 hash、schema hash、row count、最早/最晚时间逐项验证；
- catalog 更新后，已生成的 artifact 引用保持旧版本；
- 缺文件、篡改文件、未来时间和 schema 不匹配均 fail closed。

**Minimal implementation:**

- 增加 range partition 查询 API；
- 增加 `SnapshotArtifactResolver` 和流式 SHA256 校验；
- 统一使用相对 artifact path，解析后确认路径仍在允许根目录内。

**Verify:**

```bash
uv run pytest -q \
  tests/unit/test_research_snapshot.py \
  tests/unit/test_research_lake.py \
  tests/unit/test_research_catalog.py
```

## Task 3：策略依赖合同与小表不可变物化

**Files:**
- Create: `src/rquant/strategy_dependencies.py`
- Modify: `src/rquant/research_snapshot.py`
- Create: `tests/unit/test_strategy_dependencies.py`
- Modify: `tests/unit/test_research_snapshot.py`

**RED tests:**

- 三类策略依赖闭包固定且 dataset/table 唯一；
- 未知策略或未声明查询依赖拒绝正式构建；
- 小表只物化所需区间、候选和 PIT 可见行；
- materialized Parquet 保存 schema/content/file hash；
- 相同输入重复构建得到相同 manifest/binding hash；
- 构建中断不发布半成品，旧 ready binding 不受影响。

**Minimal implementation:**

- typed `StrategyExecutionDependencies` registry；
- 临时目录构建、逐文件校验、fsync 和 atomic rename；
- snapshot 路径为
  `snapshots/<snapshot_id>/<binding_hash>/manifest.json`；
- 物化 SQL 和版本进入 manifest。

**Verify:**

```bash
uv run pytest -q \
  tests/unit/test_strategy_dependencies.py \
  tests/unit/test_research_snapshot.py
```

## Task 4：不可变 ResearchExecutionSession

**Files:**
- Modify: `src/rquant/research_snapshot.py`
- Modify: `src/rquant/storage/duckdb.py`
- Create: `tests/unit/test_research_execution_session.py`

**RED tests:**

- session 打开前验证 binding 和全部 artifact；
- 现有策略表名可直接查询；
- 修改 rolling replica 或主库不影响同一 session；
- catalog head 更新不影响旧 session；
- artifact 损坏在第一条策略查询前失败；
- formal session 不允许 fallback；
- session close 后 connection 释放。
- session 打开后原 artifact 路径被原子替换，当前会话仍读取已校验 inode；重新打开则因
  hash 不符 fail closed。

**Minimal implementation:**

- 内存 DuckDB 连接；
- `read_parquet()` views 只指向 binding manifest 文件；
- 暴露 gate 和策略可共用的 context manager/store facade。

**Verify:**

```bash
uv run pytest -q tests/unit/test_research_execution_session.py
```

## Task 5：formal gate 正向路径与研究证据 v2

**Files:**
- Modify: `src/rquant/research_gate.py`
- Modify: `src/rquant/research_manifest.py`
- Modify: `src/rquant/dashboard/strategy_lab_runs.py`
- Modify: `tests/unit/test_strategy_research_gate.py`
- Modify: `tests/unit/test_research_manifest.py`
- Modify: `tests/unit/test_strategy_lab_runs.py`

**RED tests:**

- audit、coverage、ready binding 全有效时 formal 返回 `comparable`；
- metadata-only、伪造 origin、binding hash 不符、artifact 验证失败仍被阻断；
- gate decision 返回 binding hash；
- exploratory manifest 保留已知 audit/snapshot/binding 证据；
- ResearchManifest v2 正式状态要求 binding/spec/result hash；
- v1 saved run 仍可加载但不能自动晋级；
- Markdown/JSON 展示 binding 和结果证据。

**Minimal implementation:**

- 删除无条件 sentinel，替换为 binding 验证结果；
- `evaluate_store_research_gate()` 加载 binding；
- manifest 增加 v1/v2 兼容解析；
- exploratory 分支不再丢弃已有证据。

**Verify:**

```bash
uv run pytest -q \
  tests/unit/test_strategy_research_gate.py \
  tests/unit/test_research_manifest.py \
  tests/unit/test_strategy_lab_runs.py
```

## Task 6：策略覆盖率权威来源与 eligibility 证据

**Files:**
- Modify: `src/rquant/backfill_manifest.py`
- Modify: `src/rquant/backfill_state.py`
- Modify: `src/rquant/intraday_backfill.py`
- Modify: `src/rquant/cli.py`
- Modify: `tests/unit/test_backfill_planner.py`
- Modify: `tests/unit/test_backfill_state.py`
- Modify: `tests/unit/test_backfill_runner.py`
- Modify: `tests/unit/test_backfill_cli.py`

**RED tests:**

- 请求自然日范围超出权威日历时，即使零候选也失败；
- 分钟完整日优先由 research lake 版本文件证明；
- 同一股票日跨 lake/DuckDB 不重复计数；
- 缺一个请求交易日时 eligibility 不能是 100%；
- auction 成功空响应必须有完成凭证；
- 上市日前日期不发 API、不进 ETA，并保留 `not_listed` 原因；
- 零资格、零任务、任务完成但覆盖不足分别输出 execution/coverage 状态；
- coverage 未验证或失败时 CLI 非零退出。

**Minimal implementation:**

- typed `EligibilityResolution` 与稳定 resolution hash；
- typed coverage authority；
- planner 注入 research catalog/lake resolver；
- typed `UnavailableSessionReason`；
- 分离 `execution_status` 与 `coverage_status`；
- `dataset-snapshot` 使用独立 eligibility 分子/分母。

**Verify:**

```bash
uv run pytest -q \
  tests/unit/test_backfill_planner.py \
  tests/unit/test_backfill_state.py \
  tests/unit/test_backfill_runner.py \
  tests/unit/test_backfill_cli.py
```

## Task 7：snapshot build CLI 与不可变发布

**Files:**
- Modify: `src/rquant/cli.py`
- Modify: `src/rquant/research_snapshot.py`
- Modify: `tests/unit/test_backfill_cli.py`
- Modify: `tests/unit/test_research_snapshot.py`

**RED tests:**

- 预演输出可同时查看 metadata snapshot 与 execution binding 计划；
- `dataset-snapshot` 默认只输出计划；
- apply 使用精确 snapshot ID、manifest ID 和 code commit；
- 盘中保护窗口禁止扫描主库或发布 binding；
- 重复 apply 幂等；
- 构建失败不把 binding 标为 ready；
- JSON 输出包含 artifact 计数、行数、空间、binding hash 和验证状态。

**Minimal implementation:**

- 保留 `dataset-snapshot` 作为 coverage metadata 与 execution binding 的统一入口；
- 默认 dry-run，一次展示两类产物计划，只有显式 `--apply` 才原子发布；
- 调用 Task 2-4 的统一 builder/verifier。

**Verify:**

```bash
uv run pytest -q \
  tests/unit/test_backfill_cli.py \
  tests/unit/test_research_snapshot.py
```

## Task 8：Strategy Lab/worker 同会话执行与缓存绑定

**Files:**
- Modify: `src/rquant/dashboard/strategy_lab.py`
- Modify: `src/rquant/dashboard/strategy_lab_worker.py`
- Modify: `src/rquant/dashboard/strategy_lab_data.py`
- Modify: corresponding Strategy Lab unit tests

**RED tests:**

- formal gate 与策略 compute 收到同一个 session/binding；
- formal worker 全路径不调用 `open_readonly_store()`；
- exploratory 模式仍可读取 rolling replica并明确标记；
- 相同参数、不同 binding 不命中同一缓存；
- gate 通过后 artifact 被改坏，compute 拒绝；
- saved run 保存 snapshot/binding/spec/result hash；
- worker 错误清楚区分 gate、binding、artifact 和策略计算失败。

**Minimal implementation:**

- worker 只创建一次 `ResearchExecutionSession`；
- builder 不再内部重复 gate/open；
- cache key 加入 binding hash；
- 结果 canonical hash 在保存前计算。

**Verify:**

```bash
uv run pytest -q tests/unit/test_strategy_lab*.py
```

## Task 9：端到端复现、文档与真实三策略验收

**Files:**
- Create: `tests/integration/test_formal_research_reproducibility.py`
- Modify: `docs/analysis/2026-07-15-stage1-data-contract-acceptance.md`
- Modify: `docs/plans/2026-07-13-rquant-trustworthy-strategy-roadmap.md`
- Modify: `docs/plans/2026-07-13-stage1-data-contract-pit-backfill-implementation.md`
- Modify: `README.md`
- Modify: `CHANGELOG.md`
- Modify: `DEPLOY.md` only after production deployment succeeds

**RED/GREEN acceptance:**

1. fixture 构建 snapshot/binding 并跑正式策略；
2. 保存交易结果和 result hash；
3. 更新源库与 catalog head；
4. 相同 snapshot/spec 重跑结果完全相同；
5. 新 snapshot 可看到新数据并产生不同 binding；
6. 篡改旧 artifact 后正式运行 fail closed。

**Local verification:**

```bash
bash scripts/check-core-quality.sh
uv run pytest -q
```

**Production data acceptance:**

依次对 `n_shape`、`growth_board_surge`、`auction_gap`：

- 生成精确 manifest；
- 记录资格数、任务数、API 请求数、预计行数、磁盘和 ETA；
- 执行或确认已迁湖覆盖；
- 重新计算 eligibility/baseline/entry/exit；
- 构建 execution binding；
- 运行固定 smoke research；
- 记录 snapshot ID、binding hash、coverage、result hash 和首个缺失原因。

生产写入、发布与服务重启仍遵守工作日 `09:15-15:10` 保护窗口，不能以赶进度为由绕过。
`dataset-snapshot --apply` 在所有环境采用保守 ETA 门禁：即使启动时尚未进入保护窗口，只要
预计执行可能跨入该窗口也拒绝开始。外层进程不打开 DuckDB，只监督内部 worker；运行中越过
提前 60 秒的 deadline 时，以 OS timeout kill 并 wait 整个 worker，不依赖 DuckDB/C 扩展
何时返回 Python。worker 内部保留 signal 和阶段检查用于优雅退出。发布前重新解析
eligibility 并核对 resolution hash；集合竞价解析使用 manifest 内固定的 `auction_bar`
artifact，catalog 换头时也不能替换。精确候选记录物化为 binding 内的
`strategy_eligibility`，正式回放不得再读取滚动 `screen_result` 补候选。

## 完成定义

Stage 1 只有在以下条件同时满足时才完成：

- 三类策略存在可复现的真实资格分母；
- 资格完整性分母独立于可能缺行的日线事实表，精确候选随 binding 固化；
- baseline >= 95%，entry/exit >= 99%，eligibility 达到正式门要求；
- 不可变 binding 验证通过；
- formal gate 存在真实 `comparable` 正向路径；
- Strategy Lab/worker 使用同一 bound session；
- 源数据变化后旧结果仍可复现；
- 全量测试与核心质量检查通过；
- 生产验收记录、精确版本和回滚说明已写入文档。
