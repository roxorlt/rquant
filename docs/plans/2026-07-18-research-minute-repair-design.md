# 历史分钟研究湖受控修复设计

## 背景

Stage 1 的 `n_shape` 分钟回补 manifest 已完成，生产只读副本对全部策略窗口达到
100% 覆盖，但 `dataset-snapshot` 按正式研究口径重新检查时发现：

- 研究湖 baseline 覆盖 63,689 / 82,530（77.17%）。
- 研究湖 entry 覆盖 352 / 917（38.39%）。
- 研究湖 exit 覆盖 1,604 / 9,170（17.49%）。
- 共缺少 18,125 个唯一 `ts_code + trade_date` 会话，分布在 162 个交易日。

这些会话已存在于生产只读副本并通过完整 1 分钟网格检查，因此缺口属于“已有生产事实
尚未发布到研究湖”，不是行情接口未回补。现有每日 `research-ingest --recover` 要求
观察日期连续，不能安全修复权威链中间的大批历史分钟分区。

## 目标

新增受控命令：

```bash
rquant research-repair-minute \
  --manifest-id <已完成的分钟回补 manifest id>

rquant research-repair-minute \
  --manifest-id <同一 manifest id> \
  --apply \
  --plan-id <预演输出的 plan id>
```

预演只读生产副本、回补状态和研究湖，生成内容绑定计划，不请求 Tushare、不修改文件。
执行阶段在全局研究发布锁内重新生成计划；只有 `plan_id` 完全相同才批次原子发布。

## 非目标

- 不修改生产 DuckDB。
- 不调用 Tushare 或产生新的分钟行情事实。
- 不重新计算策略 eligibility，也不扩大 manifest 的既定研究窗口。
- 不绕过工作日 09:15-15:10 保护窗口。
- 不删除旧内容寻址 Parquet。
- 不把竞价分区与本次分钟修复一起发布。

## 修复范围

回补状态 SQLite 是入口，必须满足：

1. `manifest_id` 存在，内容哈希、子任务哈希和 eligibility 哈希全部通过。
2. manifest 状态为 `completed`。
3. 持久化 claim tasks 与 `MinuteBackfillPlan.tasks` 完全一致。
4. 计划包含非空、不可变的 `windows`。

需求会话集合来自持久化 `MinuteBackfillPlan.windows`：

```text
desired = windows 中全部 (ts_code, open_date)
         - manifest 已声明的 unavailable_sessions
missing = desired - 当前研究湖完整会话
```

研究湖和生产副本都按 `tushare / 1min` 的权威 session spec 检查，只有精确包含完整
09:30-11:30、13:00-15:00 共 241 个分钟时刻的会话才算完整。每一个 `missing`
必须在生产只读副本中完整；只要有一个不完整，整批计划失败。

计划不重新调用 eligibility resolver。策略资格、交易日窗口和接受缺失事实都以已持久化
manifest 为准，避免后续规则或日历变化悄悄扩大修复范围。

## 分区合并

每个受影响交易日：

1. 读取当前研究湖 `minute_bar / 1min` 分区；无分区时视为空。
2. 只从生产只读副本读取该日 `missing` 代码的 `source='tushare'`、
   `freq='1min'` 行。
3. 对每个目标代码再次验证 241 个唯一时刻、正确日期、物理主键唯一和有限 OHLCV。
4. 按物理主键 `(ts_code, trade_time, freq, source)` 合并。
5. 既有研究湖行全部保留；相同业务值保留原 `created_at`，新增行保留生产事实中真实的
   `created_at`，不得改写为修复时间。
6. 稳定排序后构建新的不可变 Parquet、manifest 和 catalog 记录。

本次只补研究湖缺失的完整会话，不以生产副本整日覆盖研究湖，因此不会意外改写与
manifest 无关的来源或代码。

## 计划绑定

`plan_id` 是规范 JSON 的 SHA256，至少绑定：

- 动作/schema 版本和干净的 40 位代码提交 SHA。
- 回补 `manifest_id`、其完整持久化内容哈希和策略窗口摘要。
- 当前 authority、主 catalog、只读 catalog 的 SHA256。
- 精确 `desired`、`unavailable`、研究湖完整、缺失和生产完整会话集合哈希及计数。
- 每个受影响日期的旧 manifest 物理哈希。
- 每日源会话键、生产业务内容哈希、合并后业务内容哈希和行数。

因此回补状态、生产事实、研究湖、catalog、权威标记或代码任一变化都会产生新的
`plan_id`，旧计划不能执行。

## 原子发布与恢复

apply 共用 `research-publish.lock`，并在锁内：

1. 恢复任何未完成的 daily、auction-repair 或 minute-repair 事务。
2. 重新加载回补状态、权威链、catalog、研究湖与生产源并生成计划。
3. 比对用户确认的 `plan_id`。
4. 复制 catalog，在事务目录中生成全部受影响分区和只读 catalog。
5. 写入包含所有 CAS 前后哈希的 minute-repair journal。
6. 发布新 immutable versions、全部 manifests、主 catalog、只读 catalog、
   repair observation 和 authority current。
7. 全部完成后删除 journal 与事务目录。

任一步失败都先完整预检全部 CAS 目标，再整体恢复旧 manifests、主副 catalog 和
authority；只删除本事务新建且原先不存在的 immutable versions。不能出现逐日部分提交。

## 权威观察

新增 `ResearchMinuteRepairObservation`：

- 指向修复前 authority observation 内容哈希。
- 绑定回补 manifest 和本次 `plan_id`。
- 逐日记录新旧 minute manifest 及其物理/逻辑哈希。
- 保持 authority 最新交易日不倒退。
- 将 `stable_trading_days` 重置为 0，不能提升为正式权威。

权威观察解析、链路验证、lake binding 和 interrupted-publish recovery 同时支持
daily、auction repair 与 minute repair。之后第一天正常日增量重新从稳定第 1 天开始。

## 运维输出

JSON 输出包括：

- `status`: `planned`、`candidate` 或 `unchanged`
- `plan_id`、`manifest_id`、策略和窗口
- desired / unavailable / lake complete / missing / source complete 数量
- 受影响日期、代码、源行、合并行和 changed 统计
- catalog/authority 前后哈希与 observation ID

首次生产修复必须先备份，保存预演 JSON，核对 18,125 个缺失会话和 162 个分区，再用
同一 `plan_id` apply。修复后重新执行 `n_shape` 的 dataset snapshot dry-run，覆盖门通过
后才 apply 正式快照与固定回放。

