# Stage 1 有界内存修复与正式冒烟验收设计

## 背景

`research-repair-minute` 当前会在预演和执行阶段把每个交易日合并后的分钟
`DataFrame` 全部保存在 `_PreparedMinuteRepair.merged_by_date`，发布前再做一次全局
`pd.concat`。N 字策略缺失 18,125 个股票交易日时，云端预演已接近 48 分钟且内存超过
2.5 GiB；成长板策略缺失约 130,228 个股票交易日，按同一实现执行很可能超过 7.5 GiB
服务器内存。

Stage 1 还缺少一个非交互、只能走正式研究门的三策略固定回放入口。现有独立 replay CLI
读取滚动库，`lab-run` 又只支持 N 字优化，不能为生产验收稳定地产出绑定
snapshot、binding、spec hash 和 result hash 的证据。

## 日期语义

策略资格截止日是移动边界，不是固定日期：

```text
latest eligibility
= latest fully closed trading session
- entry delay
- entry window
- exit window
```

新建 manifest 时，系统按权威交易日历和当时 `as_of_time` 重新计算该边界，因此数据向前
推进后资格截止日也会向前移动。已生成的 manifest 则冻结 `as_of_time`、资格集合、窗口、
输入哈希和代码提交；预演、修复和快照都不能扩大其范围。要纳入新日期必须生成新 manifest。

## 有界内存分钟修复

修复先把完整性判定改为有界批处理，再采用两遍式：

1. 研究湖和生产源的完整性查询按精确 `(ts_code, trade_date)` 目标分批，只返回计数、
   最早/最晚时刻、唯一分钟数等标量和通过的会话键；不再为每个会话把 241 个时间字符串
   聚合并物化到 Python。
2. 计划遍逐日读取研究湖已有分区和生产分钟源，验证 241 分钟完整性，计算源行、合并行和
   目标 manifest 哈希，然后立即释放当日 `DataFrame`。
3. apply 在取得发布锁并重新生成相同 plan ID 后，逐日再次读取同一来源，重建当日分区，
   逐项核对计划绑定的行数和哈希，再只把该日导出到同一个 staging catalog/lake。
4. 全部交易日 staging 完成后，统一生成只读 catalog、研究观察和 CAS journal；不可变
   version、manifest、catalog、readonly 和 authority 仍按一个事务整批发布。

单日合并使用向量化主键去重：生产补丁覆盖同物理键；若业务列空值安全地完全相同，则保留
研究湖原 `created_at`，以维持现有证据时间语义；未命中的历史行原样保留。行哈希改为增量
写入哈希器，避免同时构造整日 CSV 字符串和二次编码缓冲。

第二遍若发现源库、研究湖分区或 manifest 在两遍之间变化，必须在任何 live publication
前失败。预演仍严格零写入，plan ID 和现有回滚语义保持不变。内存上界变为“一个交易日的
研究分区 + 一个交易日的生产补丁”，不再与总日期数线性增长。

## 三策略正式冒烟回放

新增 `formal-smoke-replay`，只接受显式 `strategy`、日期区间、`audit_run_id`、
`snapshot_id` 和 `binding_hash`。命令固定使用当前干净 40 位提交构造
`ResearchGateRequest(mode="formal")`，通过 `open_gated_research_store()` 打开精确
binding；不存在 exploratory 或滚动库 fallback。

三套 v1 固定规格为：

- `n_shape`：`n-shape-combined`、`first_break`、`baseline`、持有 1 日，不做参数搜索。
- `growth_board_surge`：20 日回看、至少 10 日历史、累计量比 1.4、同刻量比 2.0、
  5 分钟加速 2.0、VWAP 强势、持有 1 日。
- `auction_gap`：收盘价口径跳空、大小写不敏感 ST 过滤、竞价量比 0.15 至 5.0、
  持有 1 日。

命令通过 Strategy Lab 既有结果模型保存 JSON/Markdown，标准输出返回 run ID、研究证据、
固定规格版本、spec hash、result hash、样本数和核心收益指标。任一正式门证据与显式参数
不一致时 fail closed。

## 验收

- 完整性判定对 exact、缺分钟、越界分钟、重复、无关代码和错误 source 的结果与旧算法
  一致，且不返回分钟字符串列表。
- 预演不得保留跨日分钟帧，也不得创建事务目录或临时 spill。
- 在子进程压力测试中，日期数扩大十倍时峰值 RSS 只随“最大单日行数”变化，不随总行数
  线性增长。
- apply 每次最多导出一个交易日，并在第二遍验证所有计划哈希。
- 中途失败不改变 live manifest、catalog、readonly 或 authority。
- 三策略命令必须证明使用同一个 bound execution session，保存 comparable v2 结果。
- 相同 snapshot/binding/spec 重跑得到相同 result hash；滚动库变化不影响旧结果。
- 生产按动态可观测截止日生成最终提交下的新 manifest，再依次修复、快照和固定回放。
