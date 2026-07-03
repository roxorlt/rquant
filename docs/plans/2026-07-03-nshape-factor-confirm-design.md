# N 字分钟级 B 点多因子确认（factor_confirm）设计

**日期**：2026-07-03
**目标**：触发器退化为宽门，B 决策改由多因子评分过阈值决定（用户 2026-07-02 确认的目标架构）。
弹药来自已验证因子（出处：docs/analysis/2026-07-02-auction-gap-feature-attribution.md、
2026-07-03-seal-hold-and-scoring-v2.md），宿主是 N 字主线的 minute_replay。

## 1. 入场架构

新增 `entry_mode="factor_confirm"`（minute_replay.MinuteReplayConfig）：

- **宽门不变**：沿用现有 `is_signal = is_strong_carry and is_break_high`（与 first_break
  同门），保证与 baseline 可比；
- **确认层**：宽门首次亮起后的每个信号分钟计算 n_shape_v1 因子组 → 画像加权分
  ≥ `factor_score_threshold` 才 `_build_snapshot`（下一分钟开盘成交，口径与现有
  模式一致）；当日始终不过阈值 = 放弃该候选。

## 2. n_shape_v1 因子组（signal_provenance 新 factor_set，键名三端同构）

| 因子 | tier | 判定 | 数据源与无未来函数依据 |
|---|---|---|---|
| auction_vol_ratio_5d | daily | >= 阈值 | B 日竞价（auction_bar，09:26 已知）/5 日均量 |
| auction_gap_pct | daily | 观察→弱权重 | 竞价价/昨收-1（对封板率 +35pp 已验证） |
| seal_open_times_t | daily | <= 阈值 | T 日官方 limit_list_daily.open_times（收盘后数据，B 日已知） |
| seal_fd_to_circ_pct_t | daily | >= 阈值(可选) | T 日 fd_amount/流通市值（daily_basic.circ_mv 万元） |
| price_percentile_250d | daily | <= 阈值（低位） | T-1 收盘（stock_features 已实现，低位方向已验证） |
| ma_alignment | daily | bool | T-1（stock_features 已实现） |
| vwap_position | minute | >= 1.0 | 信号分钟累计 VWAP（scan 内已算） |
| rel_amount_same_minute_20d | minute | **观察（op=None）** | 已证伪收益方向，只记录不判定 |
| market_above_ma20_ratio_pct | daily | **观察（op=None）** | T-1 温度，门控已证伪，只记录 |

静态（daily tier）因子每个候选**预取一次**（minute 扫描外，防止逐分钟查库）；
minute tier 在信号分钟现算。limit_list_daily 缺行（T 日非涨停不可能，但防御）
按因子缺失处理（不判定、命中矩阵记 null——与 signal_provenance「无数据不出现」约定一致）。

## 3. 评分

复用 topn_selection 的 FeatureScoreTerm 打分函数族（_score_linear 等）组装
`n_shape_b_v1` 权重表（新常量，V1 画像与既有画像逐项不动）；阈值与权重集中
一处定义。CLI：`minute-replay --entry-mode factor_confirm --factor-score-threshold X`。

## 4. 溯源

signal_provenance 加 `N_SHAPE_MINUTE_STRATEGY` / `N_SHAPE_V1` / `N_SHAPE_V1_FACTORS`；
因子命中矩阵进 ReplayEntrySnapshot 的 signal_features（已有 risk_plan.payload 通道），
落库路径复用 persist_position_with_provenance（run_mode=replay）。

## 5. 回测与验收

- 区间 2025-04-28 ~ 2026-06-30；训练 ≤2025-12-31 / 验证 2026-01 起；阈值网格
  只在训练段选（3-4 档），验证段一次性全量汇报
- 对照组：first_break / vwap_confirm baseline × n-shape-pool1 / pool2 / combined
- 指标：笔数（N 字池小，样本量必须带）/ 均收益 / 胜率 / 中位 / 最差 /
  确认层放弃率；费前结论对照 0.2-0.4% 双边成本
- 交付：实现 + 测试 + 回测 markdown + docs/analysis 报告
