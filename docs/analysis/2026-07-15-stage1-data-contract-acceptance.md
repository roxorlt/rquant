# 阶段 1 数据契约与真实数据验收

> 日期：2026-07-15
> 代码基线：`v0.16.0` / `281657a6bcf7efea9af272df208c37b1b70e32ba`，叠加 PR-D 待合并改动
> 结论：代码能力验收通过；真实数据验收被 2 个 P0 与不可变计算快照缺口阻断，阶段 1 尚未完成
>
> 2026-07-16 复验：`v0.17.2` 已完成生产数据修复，Stage 1 数据审计 P0 降为 0；策略级分钟
> 覆盖和不可变计算快照仍未完成，因此整体阶段 1 仍未签署完成。
>
> 2026-07-17 代码复验：`v0.22.0` 候选已实现不可变执行绑定、研究湖覆盖权威和同会话正式
> 计算；剩余阻断收敛为三类策略的真实 manifest、覆盖回补和固定 replay 生产验收。

## 1. 核验方式

为避免本地盘中 monitor 与研究核验争抢 DuckDB 写锁，本次从本地只读副本复制临时库，在临时
库上应用 migration v1-v8 并运行审计。没有修改本地或云端生产主库，也没有启动大规模分钟下载。

审计区间为 `2026-04-01..2026-07-14`，规则版本为 `stage1-v2`，审计 ID 为
`3ae09d2fb0c543cfc7cea81a6d0721dffc65922a5e8378f5ad192132360b7471`。

主要数据水位：

| 数据集 | 行数 | 范围/水位 |
|---|---:|---|
| `daily_bar` | 6,374,098 | 2020-08-24..2026-07-14 |
| `screen_result` | 853 | 2026-04-16..2026-07-14 |
| `limit_up_pool_daily` | 1,022 | 2026-07-02..2026-07-14 |
| `minute_bar`（Tushare） | 18,998,656 | 2025-03-28..2026-07-01 |
| `stock_status_daily` | 0 | 尚未回补 |
| `stock_suspend_coverage` | 0 | 尚未回补 |

## 2. 审计结果

共发现 5 项，其中 P0 2 项、P1 2 项、P3 1 项。`data-audit` 以退出码 1 正确阻断正式研究。

| 级别 | 规则 | 影响量 | 从最新日期向旧抽样 | 处理结论 |
|---|---|---:|---|---|
| P0 | 历史名称/ST 覆盖缺失 | 379,658 个股票日 | 2026-07-14 即命中，回退 0 天 | 必须先回补，当前策略资格按 unknown fail closed |
| P0 | 涨停池含休市日数据 | 400 行、4 个休市日 | 2026-07-12 命中，距截止日 2 个自然日 | 重新生成生产 plan 后原子修复 |
| P1 | 日线资格股票日无权威 Tushare 分钟 | 360,774 个股票日 | 区间起点即大量存在 | 不做全市场灌库，按策略资格 manifest 回补 |
| P1 | 有分钟但无匹配日线 | 558 个股票日 | 最新异常为 2026-06-26，回退 18 个自然日 | 核对退市/代码状态与日线缺口 |
| P3 | 两个实时源保存完全相同分钟 | 53,335 根 | 2026-07-03 有样本 | 可去重优化，不阻断研究 |

临时库生成的休市日修复 plan ID 为
`eadc82c622afa8d7f0fd0b4941c50d7018352587e6baf2fda6256c2243286540`。该 ID 只证明规划器可复现，
不能直接用于生产；生产执行前必须针对当时主库重新 dry-run，并显式确认新的 plan ID。

## 3. 回补规模

历史名称/ST 在该区间需要补 379,658 个资格键、69 个交易日。按当前 Tushare 适配器需要 13 个
`namechange` 时间窗请求和 69 个 `stock_st` 日期请求，共 82 个逻辑 API 操作；纯限频等待下限约
12.3 秒，实际还包含服务端响应、标准化和一次批量写入，保守按数分钟安排。

权威停复牌覆盖同样需要按 69 个交易日调用 `suspend_d`。PR-D 新增
`security-status-backfill --dry-run` 和 `suspension-backfill`，网络请求期间不持有 DuckDB 连接，
写入仍必须安排在 monitor 停止后的安全窗口。

三类策略的回补规划器均被实际调用，但资格数和任务数都是 0。这不是“没有候选”，而是
`stock_status_daily` 全空后 PIT 资格解析按 unknown 排除全部标的。因而当前不能产生有意义的
P0/P1/P2 分钟下载 manifest，也没有执行任何大规模下载。

## 4. 验收判定

代码侧已具备版本化迁移、PIT 契约、正式/探索模式隔离、数据审计凭证、精确回补规划、覆盖率
阶段门和停复牌事实链。审查确认当前 `dataset-snapshot` 只冻结覆盖元数据，不能约束实际查询的
底层数据代际；formal gate 因此会无条件报 `snapshot_execution_unbound`，不会因为手工填写
一个 origin 就把结果标成
`comparable`。当前仍不满足以下阶段出口：

- 历史名称/ST 资格分母不可用；
- 涨停池休市日污染未在权威库修复；
- 策略 B/S 99% 与历史基准 95% 覆盖率尚无可验收 manifest；
- 停复牌 coverage 尚未建立。
- 尚未实现不可变计算快照或等价的快照绑定查询。

因此阶段 1 状态是“实现完成、数据阻断”，不得进入阶段 2，也不得把现有策略从
`exploratory` 晋级。

## 5. 解锁顺序

1. 合并并发布 v0.17.0，在安全窗口按
   [首次部署前数据初始化](../deploy/2026-07-15-v0.17.0-stage1-bootstrap.md)
   用目标版本 worktree 初始化 migration v7-v8 和必需数据。
2. 回补 `2026-04-01..2026-07-14` 的停复牌和历史名称/ST；先 dry-run 核对 82 次逻辑调用。
3. 在生产库重新生成并确认 `zt-pool-repair` plan，删除 4 个休市日的 400 行污染。
4. 刷新只读副本并重新运行 `data-audit`，要求 P0 为 0。
5. 用已合并的精确 commit 重新生成三类策略 manifest；先审阅请求数、磁盘和 ETA，再单独确认下载。
6. 回补结束后固化 dataset snapshot；B/S 至少 99%、历史基准至少 95% 才允许正式回测。
7. 增加不可变数据快照/绑定查询，全量测试、CI、云端 preflight 与本地研究门同时通过后，
   才签署阶段 1 数据验收。

## 6. 2026-07-16 生产复验

PR #82、#83 合并后，腾讯云于 16:12-16:17 在交易保护窗口外部署 `v0.17.2`，精确 commit 为
`aa3d4e378d2867303681a7a553bba752f6744a07`。生产回补只重算剩余 13 个历史状态键；这些键的
Tushare `change_reason` 均为 `退市整理期`，现按已知但主动排除的边界处理，不参与策略或涨跌停
派生，也不再伪装成普通非 ST。

复验结果：

| 项目 | 结果 |
|---|---|
| 审计区间/规则 | `2026-04-01..2026-07-15` / `stage1-v3` |
| 审计 ID | `62485722f2daa4591189f88ac3d65db327ae9cef4d437f638ea9ce19cee55782` |
| 审计结论 | 4 项 finding，P0=0，状态 `completed` |
| 历史状态覆盖 | 385,183/385,183；missing/unknown/conflict/invalid 均为 0 |
| 主动安全排除 | 13 个股票日，P2，可追溯且 fail closed |
| schema | migration v9 |
| 日线水位 | `2026-07-15`，1,628,806 行 |
| 盘中水位 | 分钟 `2026-07-16 15:00`；竞价 `2026-07-16` |
| 停复牌覆盖 | 最新 `2026-07-15`，71 个交易日 |
| 主副本 | 状态覆盖、审计、排除、schema 和日线摘要完全一致 |
| preflight | `ok=5 warn=0 fail=0 skip=0` |

修复前后精确恢复点分别为
`v0.17.2-pre-apply-20260716T081302Z.duckdb.gz` 和
`v0.17.2-post-repair-20260716T081750Z.duckdb.gz`。所有 rQuant timers 已恢复，17:00 日终任务
保持原计划。

本次只关闭了生产数据 P0。审计剩余项包括策略资格股票日缺权威历史分钟、少量分钟无匹配日线、
实时来源重复分钟和上述主动排除；策略 manifest 的 B/S 99%、历史基准 95% 覆盖率以及不可变
计算快照仍是下一阶段出口条件。

## 7. 2026-07-17 不可变执行链代码复验

`v0.22.0` 候选新增 migration v10 和 `dataset_snapshot_binding`。大表绑定研究湖
`versions/<file_hash>.parquet`，小表按策略依赖、日期、候选和 PIT 截止时点物化为内容寻址
Parquet；manifest 与每个 artifact 的文件、schema、逻辑内容、行数、主键和时间范围均在查询
前验证。formal gate 与实际计算共享同一个 `ResearchExecutionSession`，不再出现门禁检查 A
代副本、计算读取 B 代副本的时序竞态。

eligibility 分母改由独立 `EligibilityResolution` 证明，权威日历中的请求、评估、完成与未知
日期分别保存；股票分母使用上市股票、历史状态和日线键并集，缺失日线本身会降低完备率，而
不会从样本中消失。精确 eligibility 记录随 binding 物化，三类正式回放只能读取该工件。
集合竞价 resolution 额外保存用于解析候选的精确 `auction_bar` artifact；规划、snapshot
发布和 formal replay 使用相同文件 hash，不跟随之后的 catalog head。
分钟覆盖正式口径改为研究湖不可变版本。上市日前与权威全日停牌会在任务和 ETA 生成前分类。
Strategy Lab 的 v2 记录必须保存 dataset snapshot、binding、策略参数和完整结果四层 hash。

本地集成验收已证明：

1. fixture 可通过正式门并从绑定执行包计算结果；
2. 更新源 DuckDB 与 catalog head 后，相同 snapshot/binding/spec 的结果和 hash 不变；
3. 篡改旧 binding 的 Parquet 后，在第一条策略查询前 fail closed；
4. `dataset-snapshot` 默认不打开写库，只有显式 `--apply` 才发布；所有环境的工作日 apply
   在 `09:15-15:10` 拒绝，也会在保守 ETA 可能跨入该窗口时拒绝；启动后由不打开 DuckDB
   的父进程监督，提前 60 秒以 OS timeout kill 并 wait 整个 worker。worker 内部另有 signal
   和阶段 deadline 复查，正常情况下先释放 DuckDB context。
5. 覆盖率计算与 binding 共用同一组内容寻址分区；会话通过私有硬链接固定已校验 inode，
   原 artifact 路径被替换不会改变本次计算。
6. formal 元数据预检不会直接晋级；只有执行会话完成全部 artifact 校验后才返回
   `comparable`。
7. snapshot 发布前重新解析 eligibility 并比对 resolution hash；候选全集已变化时要求重新
   规划 manifest，不能把旧资格证据绑定到新数据。
8. 运营 `auction_bar` 与研究湖分叉、catalog 换头时，eligibility 和 binding 仍读取 manifest
   中同一代竞价 artifact。
9. 真实阻塞子进程在 deadline 到达后被父进程终止，外层 apply 全程不打开 DuckDB。

尚未签署 Stage 1 完成：需要在生产依次对 `n_shape`、`growth_board_surge`、`auction_gap`
生成真实 manifest，记录资格数、任务数、API 请求、磁盘和 ETA，补齐 baseline 95%、
eligibility/entry/exit 99%，生成 binding 并运行固定 smoke research。只有真实结果满足这些
条件，策略记录才允许晋级 `comparable`。
