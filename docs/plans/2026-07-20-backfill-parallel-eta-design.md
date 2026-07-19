# 分钟回补并发与可信 ETA 设计

## 背景

`v0.25.1` Stage 1 生产初始化证明现有分钟回补执行器能够正确恢复、围栏和验收，但吞吐与
计划规模不匹配：

- `growth_board_surge` 共 5,493 个任务、37,252,334 行，串行执行约 10 小时；
- `auction_gap` 共 21,726 个任务、609,243 个唯一缺失会话，静态 ETA 为 8,368 秒；
- 生产实测单任务 EWMA 已达到 10.416 秒，集合竞价串行实际可能接近 60 小时；
- `backfill-run` 在 2026-07-20 06:17 正确拒绝跨越 09:15-15:10 保护窗口，原 timers
  已恢复。

静态 ETA 把理想请求延迟、传输和写入简单相加，没有使用已经落在 SQLite 的真实任务耗时；
执行器又把网络等待和 DuckDB 写入放在同一个串行循环中，因此没有利用 Tushare
`stk_mins` 每分钟 500 次的权限。

## 方案比较

### 方案 A：多个独立进程直接运行现有 worker

实现最少，但每个进程都会打开生产 DuckDB。即使任务不同，也会引入跨进程文件锁和 upsert
事务冲突；额外文件锁只能保护新 worker，不能保护其他生产写者。否决。

### 方案 B：线程 worker 并发执行现有任务，DuckDB 访问统一串行

每个线程拥有独立 Tushare adapter，通过 SQLite 原子 claim 获得不同任务。网络请求发生时
不持有 DuckDB；任务开始检查和最终 upsert/覆盖验证都经过进程内同一把锁。它复用现有
claim token、lease renew、失败隔离和断点恢复，不改变数据语义。采用。

### 方案 C：先并发下载到 staging，再按日批量导入

吞吐和可观测性最好，但需要新增 staging 格式、提交协议和清理恢复状态，改动面接近一条新
数据管道。Stage 1 收口不需要这一级复杂度，留作数据量继续扩大后的候选方案。

## 执行模型

新增一个有界 worker 编排器。默认 8 个线程，每个线程只复用自己的 `TushareAdapter`；
`BackfillStateStore` 的每次操作仍创建独立 SQLite 连接，继续使用 `BEGIN IMMEDIATE`、
claim token 和 lease fencing。所有线程共享一个 DuckDB store factory 锁：

1. 串行打开 DuckDB，检查该任务已有完整会话，然后立即关闭；
2. 释放锁后调用 Tushare，网络请求可与其他 worker 并发；
3. 请求完成后等待 DuckDB 单写锁，取得锁后再次核对软截止并续租 claim；
4. 串行重查、upsert、完整性验证并关闭 DuckDB；
5. 在 SQLite 标记成功或失败。

worker 数默认 8、上限 16。按成长板生产实测约 6.55 秒/任务计算，8 workers 约为
73 次/分钟，显著低于 500 次上限；Tushare 现有退避逻辑继续处理瞬时限频。运行器只有在
当前任务安全落库后才领取下一项。09:05 为软写截止：截止后未写数据直接丢弃，claim
无损退回 pending，最终 recovery-only claim 也保持原尝试次数与恢复语义；09:10 为
父进程硬截止，再留下 5 分钟供发布脚本验证 timers 恢复。

## ETA 与分段运行

SQLite 已保存每个成功任务的 `duration_seconds` 和请求指标。冷启动 ETA 使用相同
`source/freq/response_row_limit` 最近成功且确实请求过数据的任务耗时 P75。样本不足
32 个时，同时采用成长板生产实测冷启动下限（10.416 秒/任务、651 行/秒）。剩余串行
耗时取“按剩余比例缩放的静态 ETA”“P75 × 剩余任务数”“P75 每行耗时 × 剩余预计行数”
和冷启动下限中的较大值，再按实际有效 worker 数折算并增加 25% 并发损耗余量；最终点
估计不得低于单任务 P75、冷启动单任务下限或 API 限频墙钟下界。

CLI 新增：

```text
backfill-run --workers 8 --max-runtime-minutes 1050
```

`--max-runtime-minutes` 不是绕过保护窗口。实际截止时间永远取用户预算与下一次
09:05 中较早者；父进程在软截止 5 分钟后硬退出，09:15-15:10 内仍直接拒绝。预算结束但
manifest 未完成时返回可恢复状态，下一窗口继续同一 manifest，不重下已完成会话。

## 发布与验收

发布 `v0.25.2` 后，用 `v0.25.1` 已生成的集合竞价 manifest
`3d5893dddfa0f8cd17cddec40701c216e423e9d818697dfff9a5d71b60200d3c`
续跑。发布脚本接受显式 resume manifest；本窗口未完成时恢复原 timers 并报告 `paused`。
若工作日 15:10 前刚完成 manifest，也先逐个验证 timers active 后退出；完整三策略
snapshot、审计和固定回放只在 15:10 后继续。恢复入口在 09:05 后拒绝启动；完整发布链
内的备份、同步、修复、快照、审计与回放等外部命令统一由 GNU `timeout` 限制到下一
09:10，并在每个长步骤后重查阶段窗口。

验收必须证明：

- 至少两个网络请求真实重叠，DuckDB store 活跃数始终不超过 1；
- 每个任务只被一个 claim 完成，聚合计数与 SQLite 状态一致；
- 分段软截止不再打开 DuckDB，硬截止能终止阻塞 worker，保护窗口不能被参数绕过；
- 动态 ETA 使用生产遥测且对无样本保持保守回退；
- 完整单测、核心质量检查、Python 3.11/3.12 CI 全绿；
- 生产回补完成后再执行研究湖修复、三份 ready snapshot、P0=0 审计和 comparable 固定回放。
