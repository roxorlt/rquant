# Stage 1 收口：不可变执行快照与策略覆盖证据设计

> 日期：2026-07-17
>
> 状态：本地实现与审查修复完成，等待真实三策略生产验收
> 基线：`origin/main@006792723489265d4089e7f958d504ad4207ad85`

## 1. 为什么现在必须做

当前 Stage 1 已经有数据契约、PIT 状态、数据审计、策略资格全集、分钟回补 manifest、
覆盖率门和研究状态。但正式研究仍然没有成功路径：

- `dataset_snapshot` 只保存元数据和水位，不保存实际计算文件；
- Strategy Lab 通过 gate 后会重新打开滚动更新的只读副本；
- worker 的 gate、builder 和计算最多可跨越三个副本代际；
- `research_lake` 已保存内容寻址的分钟/竞价 Parquet，但策略覆盖规划仍只扫描
  DuckDB 的 `minute_bar`；
- eligibility 覆盖率目前用 `len/len` 自证 100%，不能证明每个请求交易日都完成了解析；
- 请求区间超出交易日历已知范围时，planner 可能静默缩短需求。

所以现有 `snapshot_execution_unbound` 不是多余限制，而是在准确阻止一个无法复现的正式回测。
删除它只会把“同一个 snapshot ID、实际读取不同数据”的结果错误晋级为 `comparable`。

## 2. 设计目标

完成后，一次正式研究必须同时固定并验证：

1. 策略、日期范围、代码提交和回补 manifest；
2. 每个所需数据集的精确不可变文件版本；
3. 资格、历史基准、B 日和 S 窗口的独立覆盖证据；
4. 研究 gate 与实际计算使用同一个已验证数据会话；
5. 保存结果可追溯到 snapshot ID、binding hash、策略参数 hash 和结果 hash；
6. 源库、只读副本或研究湖 catalog head 后续变化，不改变旧快照的计算结果；
7. 任一绑定文件丢失、被篡改、schema 不符或包含未来数据时，在计算前 fail closed。

## 3. 备选方案

### 3.1 完整 DuckDB 冻结副本

每个 snapshot 复制一份完整 DuckDB。

优点是查询改动少，能快速证明概念。缺点是每次复制大库，空间、锁、构建时间和生命周期成本
都过高，也会重复保存已经内容寻址的千万级分钟数据。

### 3.2 纯研究湖快照

把所有策略依赖表都改成分区 Parquet，由 snapshot 固定全部 partition version。

这是长期最整洁的形态，但当前研究湖只有 `minute_bar` 和 `auction_bar`。如果 Stage 1 立即要求
所有生产小表完成湖化，改动面过大，会把可信研究收口拖成一次存储平台重写。

### 3.3 混合不可变执行包

大表引用研究湖已有的内容寻址版本；小表按策略和日期依赖闭包物化成不可变 Parquet 执行包。
查询时只从绑定 manifest 建立 DuckDB views。

这是本次采用的方案。它复用现有研究湖，避免复制大库，同时能在不重写策略 SQL 的前提下固定
`daily_bar`、`daily_state`、`stock_status_daily`、`trade_calendar` 等依赖。

## 4. 核心数据契约

### 4.1 保留现有 snapshot ID

不得修改 `DatasetSnapshot.snapshot_id` 算法。历史 snapshot 行在读取时会重新计算稳定 ID，
修改算法会让已有数据全部失效。

`dataset_snapshot` 继续表达“策略、manifest、时点和代码提交的研究身份”。实际数据绑定新增
一对一子表 `dataset_snapshot_binding`。

### 4.2 执行绑定

`DatasetSnapshotBinding` 至少保存：

- `snapshot_id`
- `binding_version`
- `binding_hash`
- `artifact_root`
- `manifest_relative_path`
- `manifest_hash`
- `created_at`
- `completed_at`

binding hash 由规范化 manifest JSON 计算，不把绝对机器路径写进 hash。相同内容迁移到另一台
机器后仍是同一绑定。

绑定只有 `building -> ready` 生命周期。ready 后只允许完全相同的幂等重试，禁止覆盖。

### 4.3 执行 manifest

manifest 使用版本化 Pydantic 模型，包含两类 artifact：

- `lake_partition`：固定 `versions/<file_hash>.parquet`，保存 dataset、partition key、
  row count、schema hash、content hash、file hash、时间范围；
- `materialized_table`：保存小表名、相对路径、row count、schema hash、content hash、
  file hash、PIT 截止时点和物化查询版本。

研究湖 artifact 同时记录 revision 创建时间与 catalog 更新时间。`as_of_time` 只约束市场
事件时间和策略可见性；历史分钟数据可以在事后回补，但结果必须声明它使用的是 binding
创建时冻结的数据版本，不能声称这些文件在历史墙钟时刻已经存在。

manifest 还保存：

- snapshot ID、strategy、日期范围、as-of、code commit；
- 策略依赖合同版本；
- artifact 有序列表；
- 构建器版本；
- 规范化 manifest hash。

不得绑定可变的 `manifest.json`、catalog 当前行或 `rquant_ro.duckdb` 文件名。只允许绑定
内容寻址版本或本次执行包内的新不可变文件。

### 4.4 策略依赖闭包

每个正式策略必须注册 typed `StrategyExecutionDependencies`，第一批覆盖：

| 策略 | 大表 | 小表依赖 |
|---|---|---|
| `n_shape` | `minute_bar` | `daily_bar`、`daily_state`、`stock_status_daily`、`trade_calendar`、`adj_factor`、`screen_result` |
| `growth_board_surge` | `minute_bar` | `daily_bar`、`daily_state`、`stock_status_daily`、`trade_calendar`、`adj_factor` |
| `auction_gap` | `minute_bar`、`auction_bar` | `daily_bar`、`daily_state`、`stock_status_daily`、`trade_calendar`、`adj_factor` |

实现前应以实际查询为准收紧或补充。缺少已声明表、出现未声明表、PIT 截止晚于 snapshot
时点，都必须拒绝构建或计算。

## 5. 绑定构建流程

1. 读取 ready metadata snapshot、回补 manifest 和独立覆盖证据；
2. 验证权威交易日历完整覆盖请求自然日区间；
3. 覆盖计算从研究 catalog 解析需求内每个分钟 partition 的当前精确版本，并把实际读取过的
   artifact 清单直接交给 binding builder；竞价依赖也只解析一次；
4. 将小表依赖按策略、日期和 PIT 条件物化到临时目录；
5. 逐文件计算 schema/content/file hash，并重新读取验证；
6. 生成规范化 execution manifest；
7. fsync 文件和目录后原子发布到 `snapshots/<snapshot_id>/<binding_hash>/`；
8. 在 DuckDB 以 CAS 写入 ready binding；
9. 后续 catalog head 或源库变化只产生新 binding，不修改旧 binding。

构建失败时，snapshot 可以继续保持 metadata-only，但正式 gate 必须报告
`snapshot_execution_unbound` 或更精确的 artifact 错误。

## 6. 执行会话

新增 `ResearchExecutionSession`：

- 创建时读取 binding，验证 manifest 和全部 artifact；
- 校验后在研究湖内建立会话私有硬链接，view 只读取私有 inode；原内容寻址路径被原子替换
  不影响已打开会话，关闭时清理私有链接；
- 建立一个内存 DuckDB connection；
- 以现有物理表名创建只读 views；
- 暴露与策略现有读取方式兼容的 store/query 接口；
- 会话关闭前，gate 与计算共享同一 connection 和 binding hash；
- formal 模式严禁 fallback 到 rolling replica 或主库；
- exploratory 模式仍可读 rolling replica，但结果必须保留已有 audit/snapshot 证据并标为
  `exploratory`。

这消除了“gate 看 A 代副本，计算看 B 代副本”的 TOCTOU。

## 7. 覆盖证据修正

### 7.1 研究湖成为分钟覆盖权威来源

`_complete_minute_sessions()` 不能只扫描生产 DuckDB。云端迁移后，完整历史分钟的权威来源是
研究湖 content-addressed partition。

覆盖 resolver 应：

- 优先从 catalog + 版本文件验证完整交易日；
- 本地尚未迁湖的数据可通过显式配置扫描 DuckDB，但必须记录 authority；
- 不允许把两个来源重复计数；
- snapshot 覆盖记录保存 authority 和证据 hash。

### 7.2 eligibility 不能自证

引入 `EligibilityResolution`，至少保存：

- requested trade dates；
- evaluated trade dates；
- complete trade dates；
- incomplete/unknown dates 及原因；
- eligibility rows；
- resolution hash。

`dataset-snapshot` 的 eligibility 分母是请求交易日或应评估单元，分子是独立证明完成的单元，
不能继续用 `len(eligibilities)` 同时作为分子和分母。

首批独立完备性定义：

- `n_shape`：121 个交易日日线事实面板中的 OHLC、状态、历史 ST 可见性至少 99%，信号日
  `daily_basic.circ_mv` 至少 99%；
- `growth_board_surge`：前一交易日科创/创业板日线全集中，信号时点可见的历史 ST 与均线
  输入至少 99%；
- `auction_gap`：前一交易日日线股票全集中，09:27 可见且有效的竞价与历史 ST 输入至少
  99%。一条竞价记录不能证明整日解析完成。

### 7.3 日历与不可行动缺失

- manifest 请求自然日范围必须全部存在于权威 SSE 日历，缺一日就 fail closed；
- 上市日前日期在 planner 阶段分类为 `not_listed`，不请求 API、不进入 ETA；
- 停牌、未上市和数据源异常必须是不同 typed 原因；
- `execution_status` 与 `coverage_status` 分离。任务跑完不等于覆盖验收通过。

## 8. 正式研究门

formal gate 只有在以下条件全部满足时返回 `comparable`：

- clean code commit；
- 当前规则版本的 completed audit 覆盖研究区间且 P0=0；
- ready snapshot 与策略、区间、commit 一致；
- ready binding 存在且 manifest/hash/artifact 全部验证通过；
- snapshot、binding 和 `strategy_eligibility` 工件的 resolution hash 与日期计数一致；
- 集合竞价 resolution 声明精确输入 artifact，候选解析与 formal replay 使用相同文件 hash；
- eligibility、baseline、entry、exit 分母非零并达到阈值；
- 计算使用 gate 返回的同一个 `ResearchExecutionSession`。

页面预检只判断审计、覆盖率和绑定元数据，显示“等待执行时文件校验”；正式点击后必须重新
校验全部文件。仅当该会话成功打开，执行结果携带的 decision 才能保存为 `comparable`。

`ResearchGateDecision` 新增 `dataset_binding_hash`。`ResearchManifest` 升级 schema v2，并保存：

- dataset snapshot ID；
- dataset binding hash；
- strategy spec hash；
- execution model version；
- cost model version；
- result hash。

旧 v1 记录继续可读，但只能保持原状态或降级，不能因为新增字段默认值而自动升级。

## 9. 缓存、同步和生命周期

- Strategy Lab cache key 必须包含 snapshot ID、binding hash、代码 commit 和参数 hash；
- `dataset_snapshot_binding` 随 metadata 一起在云端/本地同步，不能只同步父表；
- 执行包文件通过研究数据迁云链路传输并逐文件验 hash；
- 任何清理/GC 都必须先检查 snapshot binding 和 saved run 引用；
- 绑定文件不可原地修复。损坏时构建新 binding，并保留旧绑定的失败证据。

## 10. 非目标

本次不做：

- 新策略或新因子；
- 策略参数自动调优；
- 实盘下单；
- Tick/Level2；
- 将全部生产数据一次性重构为数据湖；
- Strategy Lab 视觉重做。

这些工作必须等 Stage 1 正式研究成功路径和真实三策略覆盖验收完成后再继续。

## 11. 验收标准

1. 相同 snapshot + binding + spec 重跑两次，结果 hash 相同；
2. 更新源 DuckDB 和 catalog head 后，旧 snapshot 结果不变；
3. 修改任一绑定文件一个字节，正式计算在策略执行前失败；
4. formal worker 全路径不调用 `open_readonly_store()`；
5. 请求区间超出交易日历、eligibility 未完整解析、覆盖分母为零时均非零退出；
6. 上市股票缺日线时仍保留在资格完整性分母，不能因源表缺行而被静默排除；
7. 三类正式策略只消费 binding 内的精确 eligibility 键，键缺失或不一致时失败关闭；
8. apply 的保守预计结束时间不得跨入工作日交易保护窗口；
9. apply 外层进程不得打开 DuckDB；worker 越过提前 60 秒的 deadline 时，父进程必须用
   OS timeout 终止并回收 worker；
10. 运营竞价表和 catalog head 变化后，旧 resolution 仍使用原 `auction_bar` artifact；
11. 三类策略生成真实 manifest、执行覆盖验收并记录精确证据；
12. 全量测试、ruff、preflight 和固定 replay smoke 全绿。
