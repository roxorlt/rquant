2026-07-29 16:05:04.086 | INFO     | rquant.adapter.tushare:stk_mins:384 - Tushare stk_mins 请求：code=003040.SZ freq=1min start=2026-07-29 09:00:00 end=2026-07-29 15:30:00
2026-07-29 16:05:04.189 | WARNING  | rquant.adapter.tushare:stk_mins:400 - Tushare stk_mins 返回空：code=003040.SZ 1min 2026-07-29 09:00:00-2026-07-29 15:30:00
2026-07-29 16:05:04.189 | INFO     | rquant.adapter.tushare:rt_min_daily:509 - Tushare rt_min_daily 请求：code=003040.SZ freq=1MIN
2026-07-29 16:05:04.223 | INFO     | rquant.adapter.tushare:rt_min_daily:557 - Tushare rt_min_daily 返回 241 行
2026-07-29 16:05:04.239 | INFO     | rquant.adapter.tushare:stk_mins:384 - Tushare stk_mins 请求：code=300750.SZ freq=1min start=2026-07-29 09:00:00 end=2026-07-29 15:30:00
2026-07-29 16:05:04.275 | WARNING  | rquant.adapter.tushare:stk_mins:400 - Tushare stk_mins 返回空：code=300750.SZ 1min 2026-07-29 09:00:00-2026-07-29 15:30:00
2026-07-29 16:05:04.275 | INFO     | rquant.adapter.tushare:rt_min_daily:509 - Tushare rt_min_daily 请求：code=300750.SZ freq=1MIN
2026-07-29 16:05:04.308 | INFO     | rquant.adapter.tushare:rt_min_daily:557 - Tushare rt_min_daily 返回 241 行
2026-07-29 16:05:04.322 | INFO     | rquant.adapter.tushare:stk_mins:384 - Tushare stk_mins 请求：code=600519.SH freq=1min start=2026-07-29 09:00:00 end=2026-07-29 15:30:00
2026-07-29 16:05:04.351 | WARNING  | rquant.adapter.tushare:stk_mins:400 - Tushare stk_mins 返回空：code=600519.SH 1min 2026-07-29 09:00:00-2026-07-29 15:30:00
2026-07-29 16:05:04.351 | INFO     | rquant.adapter.tushare:rt_min_daily:509 - Tushare rt_min_daily 请求：code=600519.SH freq=1MIN
2026-07-29 16:05:04.397 | INFO     | rquant.adapter.tushare:rt_min_daily:557 - Tushare rt_min_daily 返回 241 行
# iTick 一致性报告 2026-07-29

tick 总条数 12340，标的 ['003040.SZ', '300750.SZ', '600519.SH']

## 端到端延迟 (ms)

P50 1841 | P95 2654 | P99 3225 | max 13799

## 断线事件：2 次

- 14:37:35 disconnected: code=None msg=None
- 14:37:35 reconnect: 1s 后重连（断线期间数据永久丢失）
- 15:12:01 disconnected: code=None msg=None（计划内 15:12 停录）

## 003040.SZ（tick.v 判定：单笔量 | 推送间隔中位 3.0s → ~3 秒级快照聚合流（非逐笔））

聚合 1min bar 241 根，首根 09:25，末根 15:00
聚合当日总量 162,777

> 参照源:rt_min_daily(stk_mins 当日尚无数据)

| 指标 | 值 |
|---|---|
| 可对齐分钟数 | 239 |
| open/close 完全一致比例 | 99.6% |
| 量比中位（iTick v 判定为「手」,已 ×100 归一） | 1.000 |
| 量比 P5–P95 | 1.000 – 1.000 |
| **完整性判定** | **✅ 成交量完整（可用于聚合/footprint）** |

## 300750.SZ（tick.v 判定：单笔量 | 推送间隔中位 3.0s → ~3 秒级快照聚合流（非逐笔））

聚合 1min bar 241 根，首根 09:25，末根 15:00
聚合当日总量 404,322

> 参照源:rt_min_daily(stk_mins 当日尚无数据)

| 指标 | 值 |
|---|---|
| 可对齐分钟数 | 239 |
| open/close 完全一致比例 | 99.2% |
| 量比中位（iTick v 判定为「手」,已 ×100 归一） | 1.000 |
| 量比 P5–P95 | 1.000 – 1.000 |
| **完整性判定** | **✅ 成交量完整（可用于聚合/footprint）** |

## 600519.SH（tick.v 判定：单笔量 | 推送间隔中位 3.0s → ~3 秒级快照聚合流（非逐笔））

聚合 1min bar 239 根，首根 09:25，末根 15:01
聚合当日总量 62,327

> 参照源:rt_min_daily(stk_mins 当日尚无数据)

| 指标 | 值 |
|---|---|
| 可对齐分钟数 | 237 |
| open/close 完全一致比例 | 79.3% |
| 量比中位（iTick v 判定为「手」,已 ×100 归一） | 1.000 |
| 量比 P5–P95 | 0.975 – 1.021 |
| **完整性判定** | **✅ 成交量完整（可用于聚合/footprint）** |

## 一句话结论

- **延迟** P50 1.8s / P99 3.2s → 秒级延迟——对 3 秒级封单监控勉强可用,对更快的执行不可用
- **003040.SZ** 量比中位 1.000（iTick v 判定为「手」,已 ×100 归一） → ✅ 成交量完整（可用于聚合/footprint）
- **300750.SZ** 量比中位 1.000（iTick v 判定为「手」,已 ×100 归一） → ✅ 成交量完整（可用于聚合/footprint）
- **600519.SH** 量比中位 1.000（iTick v 判定为「手」,已 ×100 归一） → ✅ 成交量完整（可用于聚合/footprint）

> 边界归属：若整体错位 1 根，把 _aggregate 的 label/closed 换成 left 重跑即可确认 iTick 的 bar 时间戳约定。
