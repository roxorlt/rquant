# 因子收益的收盘后可见性

**分级：普通任务。** 只修正纯 `rquant.factor.result` 的收益缺失合同和聚焦测试。当前 `return_end_at` 表示价格窗口结束，但历史回溯适配器规定结果最早在**下一 SSE 开市日 09:25**可见；两者之间不能给值，也不能误称窗口还没结束或来源丢失。

## 行为合同

1. `FactorForwardReturn` 增加封闭缺失原因 `visibility_pending` 与仅供该状态使用的 `expected_available_at`。当 `return_end_at <= as_of < expected_available_at` 时，值必须为空、`first_available_at` 为空，结果逐日覆盖明确计入“待可见”；`expected_available_at` 必须晚于收益终值时刻。完整价格窗口但未到发布假设时刻，不伪装成 `window_unfinished` 或 `source_unavailable`。
2. `window_unfinished` 仅用于 `as_of < return_end_at`；当 `as_of >= expected_available_at` 时，`visibility_pending` 必须拒绝，调用方须给已可用值或实际缺失原因。已可用值仍须携带 `first_available_at`，它不早于窗口终值、也不晚于请求 `as_of`。缺价格、停牌及来源缺失的现有合同保持。
3. 纯组装器只消费调用方给出的时间和封闭状态，不自行推算交易日历或真实采集时间。后续历史适配器用冻结 SSE 日历生成下一开市日 09:25，并在来源回执标为模拟可见性；当时采集模式须另用真实归档时刻。

## 红测与验收

- 周五 15:00 价格窗口结束，下一 SSE 开市日周一 09:25 才可见：周六及周一 09:24 的请求接受 `visibility_pending`，保留因子覆盖但不给 IC/分组收益；周一 09:25 同一 pending 输入被拒，已可用值可以进入结果。以时区明确的时刻验证边界。
- 拒绝 pending 缺 `expected_available_at`、预期时刻不晚于终值、窗口尚未结束却声称 pending、其他缺失原因夹带预期时刻，以及已可用值在首次可见前进入请求。既有 `window_unfinished`、实际缺价格和极端合法收益路径保持。
- 运行因子结果与直接依赖聚焦测试、Ruff、diff；共享测试清单与进度台账留到集成分支统一登记。最终候选一次独立终审，普通任务最多一次定向修复/复核。
