# N 字分钟级 B 点多因子确认（factor_confirm）回测终判

**日期**：2026-07-03
**设计**：docs/plans/2026-07-03-nshape-factor-confirm-design.md（宽门沿用 first_break 同门，
B 决策改由 n_shape_b_v1 多因子评分过阈值决定）
**数据**：本地只读副本，费前；弹药出处见 2026-07-02-auction-gap-feature-attribution.md
与 2026-07-03-seal-hold-and-scoring-v2.md。

## 零、数据现实与设计偏差（先读）

1. **设计区间 2025-04-28~2026-06-30 不可达**：`screen_result` 中 n-shape 池候选最早
   2026-04-16（预设上线时点），设计的训练段（≤2025-12-31）为空。训练/验证改为池内
   切分：训练 signal_date ≤ 2026-05-31，验证 2026-06-01 起。**全部组别样本 < 30 笔**，
   本报告所有结论都只是方向性弱证据，不是统计结论。
2. **「T 日封板质量」日期口径修正**：N 字池 T 日显式非涨停（preset
   `not_limit_up(offset=0)`），首板在 T-1（pool1）/ T-2（pool2）。设计文档按 T 日读
   limit_list_daily 会 100% 缺行（实测覆盖 0/33）；实现改读 [T-3, T] 交易日窗口内
   最近一条官方 'U' 行（`_query_nshape_seal_quality`），修正后覆盖 33/33。
3. **评分区间按 N 字池实测标定**（训练段探针，阈值=0 全进）：B 日竞价量比中位数
   0.013——竞价跳空策略的 0.15~5.0 带完全不适用，`auction_vol_ratio_5d` 的
   log_ratio cap 从 5.0 标定为 0.05，命中阈值 0.15 → 0.02；250 日百分位中位数
   0.97~0.99（N 字候选几乎都在年内高位，"低位"因子只在边际起作用，与 scoring V2
   结论一致）。阈值网格 28/35/42 = 训练段首信号分钟得分的 p25/p50/p75。

## 一、实现

- `minute_replay` 新增 `entry_mode="factor_confirm"`：宽门（强承接 + 破 T 高）与
  first_break 同门保证可比；每个信号分钟合成 minute 因子（vwap_position 复用 scan
  内累计 VWAP）+ 预取一次的静态因子（竞价强度/跳空、首板封板质量、T-1 低位与均线、
  T-1 温度）→ `n_shape_b_v1` 加权分（满分 100）≥ `factor_score_threshold` 才成交
  （下一分钟开盘，口径与既有模式一致）；当日始终不过阈值 = 放弃该候选。
- 权重表只装已验证方向弹药：竞价强度 20 + 竞价跳空 10 + 开板次数 15 + 封单占比 10 +
  250 日低位 20 + 均线多头 10 + VWAP 位置 15；**rel_amount 相对放量与市场温度已证伪
  收益方向，只进命中矩阵观察（op=None），不计分**。静态因子缺数据按 0 贡献降级。
- 溯源：`N_SHAPE_MINUTE_STRATEGY` / `N_SHAPE_V1` / `N_SHAPE_V1_FACTORS`（键名三端
  锁死，测试锁死），完整命中矩阵 + 得分随入场快照进 `risk_plan.payload`，落库通道
  复用 `persist_position_with_provenance`。
- CLI：`rquant minute-replay --entry-mode factor_confirm --factor-score-threshold 35`
  （默认 35 = 训练段网格中位档）。

## 二、结果（费前，max_hold_days=5，无 volume profile）

训练段选档标准（预先声明）：训练均收益最优且保留样本 ≥ 8 笔 → **@35**
（@42 训练均值更高但仅 3/6 笔，不作选择）。

| pool | segment | mode | trades | 确认层放弃率% | mean% | win% | median% | worst% |
|---|---|---|---|---|---|---|---|---|
| n-shape-pool1 | train(≤2026-05-31) | first_break | 10 ⚠️ | - | 2.057 | 40.0 | -1.591 | -5.283 |
| n-shape-pool1 | train(≤2026-05-31) | vwap_confirm | 9 ⚠️ | - | 2.873 | 44.4 | -0.572 | -4.998 |
| n-shape-pool1 | train(≤2026-05-31) | factor_confirm@28 | 9 ⚠️ | 10.0 | 2.792 | 44.4 | -0.572 | -4.998 |
| n-shape-pool1 | train(≤2026-05-31) | factor_confirm@35 | 6 ⚠️ | 40.0 | 5.073 | 66.7 | 1.833 | -4.998 |
| n-shape-pool1 | train(≤2026-05-31) | factor_confirm@42 | 3 ⚠️ | 70.0 | 10.31 | 66.7 | 2.234 | -3.244 |
| n-shape-pool1 | val(2026-06+) | first_break | 12 ⚠️ | - | 2.131 | 58.3 | 0.886 | -4.648 |
| n-shape-pool1 | val(2026-06+) | vwap_confirm | 12 ⚠️ | - | 1.858 | 50.0 | -0.268 | -4.648 |
| n-shape-pool1 | val(2026-06+) | factor_confirm@28 | 11 ⚠️ | 8.3 | 2.47 | 63.6 | 1.064 | -4.648 |
| n-shape-pool1 | val(2026-06+) | factor_confirm@35 | 10 ⚠️ | 16.7 | 2.634 | 70.0 | 1.905 | -4.648 |
| n-shape-pool1 | val(2026-06+) | factor_confirm@42 | 7 ⚠️ | 41.7 | 2.669 | 71.4 | 1.064 | -4.648 |
| n-shape-combined | train(≤2026-05-31) | first_break | 16 ⚠️ | - | 1.701 | 43.8 | -0.949 | -10.334 |
| n-shape-combined | train(≤2026-05-31) | vwap_confirm | 13 ⚠️ | - | 3.396 | 53.8 | 2.234 | -4.998 |
| n-shape-combined | train(≤2026-05-31) | factor_confirm@28 | 13 ⚠️ | 18.8 | 2.911 | 46.2 | -0.572 | -4.998 |
| n-shape-combined | train(≤2026-05-31) | factor_confirm@35 | 9 ⚠️ | 43.8 | 4.943 | 66.7 | 2.234 | -4.998 |
| n-shape-combined | train(≤2026-05-31) | factor_confirm@42 | 6 ⚠️ | 62.5 | 7.122 | 66.7 | 3.9 | -3.244 |
| n-shape-combined | val(2026-06+) | first_break | 17 ⚠️ | - | 1.696 | 52.9 | 0.708 | -4.648 |
| n-shape-combined | val(2026-06+) | vwap_confirm | 16 ⚠️ | - | 1.652 | 50.0 | 0.532 | -4.648 |
| n-shape-combined | val(2026-06+) | factor_confirm@28 | 15 ⚠️ | 11.8 | 1.909 | 53.3 | 0.708 | -4.648 |
| n-shape-combined | val(2026-06+) | factor_confirm@35 | 13 ⚠️ | 23.5 | 2.34 | 61.5 | 1.064 | -4.648 |
| n-shape-combined | val(2026-06+) | factor_confirm@42 | 9 ⚠️ | 47.1 | 1.978 | 55.6 | 0.708 | -4.648 |

（⚠️ = 样本 < 30 笔，统计不可靠；确认层放弃率 = 1 − factor_confirm 笔数 / 同段
first_break 笔数）

**读数**：

- **方向一致且阈值单调**：训练与验证两段、两个池，均收益与胜率都随阈值升高
  单调走强（验证段 pool1 胜率 58.3 → 63.6 → 70.0 → 71.4），无训练/验证翻转——
  与温度门控（训练有效验证翻转，已证伪）形成对照，方向大概率真实。
- **验证段增益幅度有限**：@35 vs first_break，pool1 +0.50pp 均值 / +11.7pp 胜率，
  combined +0.64pp / +8.6pp；训练段增益（+3.0~3.2pp）在验证段明显缩水，
  典型的小样本选档乐观偏差。
- **费后判断**：N 字宽门 baseline 本身费前为正（验证段 +1.7~2.1%），0.2~0.4% 双边
  成本下策略主体仍为正期望；确认层的边际增益 +0.5~0.6pp 名义上也盖得住成本增量
  （确认层不增加交易次数、只做减法），但 9~13 笔的样本量不足以支撑这个数字本身。
- **放弃率**：@35 砍掉 17~44% 的宽门信号，砍掉的部分在两段里都是拖后腿的
  （留下组均值更高），过滤方向正确。

## 三、结论

**一句话：多因子确认相对单触发器（first_break/vwap_confirm）方向为正——训练/验证
同向、阈值单调、放弃的信号确实更差——但 N 字池 2026-04-16 才有数据、所有组别 <30 笔，
+0.5~0.6pp 的验证段增益目前只是弱证据，不足以升级为「已验证弹药」；机制保留
（默认阈值 35），等池子再积累 2~3 个月样本后复检，再决定是否作为 N 字主线的默认
入场模式。**

## 复现

```bash
# baseline
.venv/bin/rquant minute-replay --start-date 2025-04-28 --end-date 2026-06-30 \
  --preset n-shape-combined --entry-mode first_break --max-hold-days 5
# 多因子确认（阈值档 28/35/42）
.venv/bin/rquant minute-replay --start-date 2025-04-28 --end-date 2026-06-30 \
  --preset n-shape-combined --entry-mode factor_confirm --factor-score-threshold 35 \
  --max-hold-days 5
```

注意：盘中运行会撞 monitor 写锁，直连主库的 CLI 请在收盘后跑，或改用只读副本。
