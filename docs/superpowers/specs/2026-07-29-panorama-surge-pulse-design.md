# 全景页爆量图表与脉搏异动 设计文档

- 日期：2026-07-29
- 分支：`cc/pano-board-surge-pulse`（基于 origin/main v0.27.1 / 96a01ff）
- 状态：待用户评审
- 需求来源：用户 2026-07-29 提出的 7 条需求（编号沿用原文）

## 0. 需求总览与处置

| # | 需求 | 处置 |
|---|------|------|
| 1 | 爆量记录下也能查看个股图表 | **本批实现**（设计 A） |
| 2 | 爆量打开的个股图在分时/5日上标注首次触发时间点 | **本批实现**（设计 A） |
| 3 | 📈 浮层脉搏曲线太平直，增减变化要看得清 | **本批实现**（设计 B，分面小图方案，用户已选定） |
| 4 | 脉搏数据异动时页面提醒 + push 通知 | **本批实现**（设计 B，四类异动，用户已全选） |
| 5 | 肉眼爆量却没进爆量记录，排查原因 | **已排查出根因**，处置为检测范围全开（设计 C，用户已选「直接全开」） |
| 6 | 个股图成交量没区分买卖方向，是数据不支持吗 | **答复见 §1.2**；落地 tick-rule 近似上色（设计 D） |
| 7 | iTick 数据能否渲染 VP / LC / 盘口深度看板 | **答复见 §1.3**；本批不做，等证伪报告另立项 |

## 1. 三个问题的结论性答复

### 1.1 需求 5：爆量漏报根因（已用真实数据验证）

用户给出的例子是「昨天和今天的东百」。东百集团是 `600693.SH`，**上海主板**票。akshare 日线核验：7/28 涨停 +9.96%（成交额 3.22 亿），7/29 再涨停 +9.99% 且成交额 10.68 亿、换手 13.54%——相比前几日 2-3 亿放大 4-5 倍，肉眼爆量确凿。但 surge-watch 的检测范围默认只有创业板 + 科创板（`surge_watch.py` 的 `_DEFAULT_SURGE_BOARDS = ("gem", "star")`），**主板票无论多爆都不会进入检测**，这就是漏报根因。

其他会造成「看着爆量但没收录」的门槛（均为刻意设计，本批不改）：

1. `rel_cum > ratio_cap(8.0)` 视为极端出货毒尾主动不推（2026-07-06 回测：11-20× 扎堆负收益组）；
2. 确认时要求当分钟上涨且 tick-rule 外盘>内盘，放量瞬间回调的票会推迟确认，转跌后不再确认；
3. 次新股缺 20 日均额基线直接落选；ST 排除；
4. 确认层 tushare 限频 2 只/分钟，候选多时排队，取数失败重试 3 次后静默丢弃。

### 1.2 需求 6：成交量买卖方向

分时/5日的数据源是东财 trends2（兜底新浪分钟），每分钟只有价格、均价、总量三个字段，**没有买卖方向，确实是数据不支持**。精确内外盘需要逐笔成交数据。可行的近似是 tick-rule（该分钟收涨记买、收跌记卖），与 surge-watch 确认层的「外盘占优」门同口径。本批按此近似给量柱上色（设计 D）。iTick 的 WebSocket tick 流带真实买卖方向字段 `d`，但 Free 档只有 3 只订阅名额，不能覆盖全市场。

### 1.3 需求 7：iTick 与 VP / LC / 盘口深度看板

数据形态上支持：iTick WS 提供 tick（带买卖方向）、quote、depth（带挂单量和委托笔数）三类推送。但有三个硬前提：

1. **证伪测试还没出结论**。云端 82.156.0.68（lighthouse 用户）`/home/lighthouse/itick_probe/` 的采集脚本正在跑，订阅的是 600519 茅台 + 300750 宁德时代 + 003040 低流动小票三只（Free 档上限），每交易日 15:20 把报告摘要直推 PushDeer，完整报告在云端 `/home/lighthouse/itick_probe/report-YYYY-MM-DD.md`。depth 是真十档还是 3 秒五档快照、成交量是否全量推送，都要以报告为准（任一不达标即放弃，见 `docs/vendor/itick/EVALUATION.md`）。
2. Free 档 1 连接 / 3 订阅，看板只能覆盖这 3 只标的；扩标的要上付费档（Base $79/月 200 订阅起）。
3. VP 规格书（`docs/vp-strategy/VP-STRATEGY-SPEC.md`）明确这类看板**不走 Streamlit**（高频刷新撑不住），要独立 canvas 页 + collector 经 SSE/WS 喂数，且写死「数据源没证实之前不为它写一行代码」（W4 证伪 → W7 数据平台 → W8 看板的串行链）。

结论：本批只做此答复，看板等证伪报告出来后另立项。届时需要一并更新 `CLAUDE.md` 中「暂不做 Tick 级微观结构」的边界声明（与 7/28 决策存在冲突，规格书已标注）。

## 2. 架构决策（用户已确认）

脉搏历史记录 + 异动检测**挂在云端 surge-watch 主循环里**（方案 A）：surge-watch 每分钟本就拉全市场快照（已含涨停价），顺手计算脉搏并落盘、检测异动、推送。理由：零新进程、单写者、告警权威留在云端盘中进程、全景页保持纯只读。被否方案：记在全景页 poller（Streamlit 重启丢历史、UI 进程兼职告警）；新开独立 systemd service（过度设计）。

## 3. 设计 A：爆量记录 tab 个股图表 + 首次触发标记

### 3.1 交互

- `render_surge_log()` 的台账表格加 `on_select="rerun"` + `selection_mode="single-row"`（复用 `_first_selected_row`），表高 520 → 300，选中行下方渲染个股图表。
- 图表复用 `render_stock_chart(ts_code, name, snapshot)`，分时/5日/日K 三周期不变。周期切换 `st.segmented_control` 的 key 参数化（`chart_period_{context}`），避免与市场全景 tab 的同名控件撞 DuplicateWidgetID。
- 未选行时显示「点选记录查看图表」提示（与下钻成分表交互一致）。

### 3.2 首次触发标记

- `_trend_chart` 加可选参数 `marks: list[SurgeMark] | None`（`SurgeMark` 为 Pydantic 模型：`dt`、`label`、`rel_cum`）。渲染为橙色（`#f97316`）竖直虚线 `mark_rule` + 价格线上 `mark_point`，tooltip 显示「HH:MM 首次爆量确认 · N.N×」。
- 时刻 → bar 序号映射：在 trend 数据里找同日内 `dt` 分钟 == 触发分钟的行；精确分钟缺失（数据缺根）时取 ≤ 触发分钟的最近一根；当天完全无数据则跳过该标记。
- 分时图（ndays=1）标当日首次触发，数据即台账行的 `confirmed_at`；5日图（ndays=5）新增 loader `load_surge_marks(ts_code, dates)`——`dates` 直接取 trend 数据里实际出现的交易日集合（避免按自然日回推漏掉周末/节假日），逐日读 `surge_live/events-YYYY-MM-DD.jsonl`，每天取该票 `confirmed_at` 最早一行（`confirmed` 与 `unbuyable` 都算）。文件缺失/坏行跳过，语义与 `load_surge_log` 一致。
- 日K 不标（一根 bar 就是一天，标记无意义）。
- 市场全景 tab 的个股图表**同样受益**：选中的票若当日在台账里，也画标记（组件级能力，两个 tab 共享）。

### 3.3 已知限制

events 文件只存在于云端 82.156.0.68（lighthouse 用户）的 rQuant data 目录，因此标记只在云端全景页（`http://82.156.0.68:28080/`）生效；本地 Mac 起的全景页读不到，标记为空但不报错。

## 4. 设计 B：脉搏历史服务端化 + 分面小图 + 异动检测

### 4.1 记录（surge-watch 侧）

- 主循环每分钟落完 `snapshot_full.parquet` 后，用全市场快照算 `compute_market_pulse`（复用 panorama_data 现有函数，快照已含涨停价），append 一行 JSON 到 `surge_live/pulse-YYYY-MM-DD.jsonl`：`{"t": "HH:MM", "limit_up": int, "limit_down": int, "broken": int, "up": int, "down": int, "up_ratio_pct": float, "total": int}`。
- append-only 落盘天然支持进程重启续写；异动检测的滑窗状态从当日文件 seed 恢复。
- 快照 miss 的分钟不落（与 events 语义一致：没有数据就没有记录）。

### 4.2 异动检测（新模块 `src/rquant/pulse_watch.py`）

- 纯内存滑窗类 `PulseAnomalyWatcher`（无 IO，可单测直接驱动），每分钟喂入当前脉搏计数，维护当日分钟序列，对比 `now` 与 `10 分钟前`（不足 10 分钟用最早可用点）：

| 规则 | 默认阈值 | 方向语义 |
|------|---------|---------|
| 涨停潮 | 涨停家数净增 ≥ 5 | 情绪升温 |
| 炸板潮 | 炸板数新增 ≥ 3 | 情绪退潮 |
| 跌停潮 | 跌停家数净增 ≥ 3 | 杀跌 |
| 涨跌占比突变 | 上涨占比变化绝对值 ≥ 15 个百分点 | 普涨普跌切换 |

- 每类规则独立 30 分钟冷却；阈值收进 Pydantic `PulseConfig`（挂在 `SurgeConfig` 旁），上线后可调。
- 误报防护：开盘后前 10 分钟（窗口不足期）只记录不告警——9:30 起各计数从零快速爬升属于数据自然稳定过程，不是异动。
- 触发后：① 走 surge-watch 已有 `notify_fn` 推 PushDeer（notify 层新增 `pulse_alert` 场景，注册进场景开关）；② append `surge_live/pulse_alerts-YYYY-MM-DD.jsonl`（含触发时刻、类型、前后值、当前全量计数）。
- 推送文案示例：标题「脉搏异动 14:32 炸板潮」，正文「炸板 10 分钟 2 → 6（+4）｜当前 涨停 41 / 跌停 3 / 上涨占比 47%」。

### 4.3 UI（全景页侧）

- 📈 浮层改为读 `load_pulse_history()`（新 loader，读当日 pulse jsonl，`st.cache_data` ttl 60s）画 **4 张分面小图**：涨停、炸板、跌停、上涨占比，各自独立 y 轴且 `zero=False`，x 轴 HH:MM 稀疏刻度（约每 30 分钟一个）。浮层内容变高（4 × 约 70px）。
- pulse 文件读不到（本地 Mac 场景）时退回现有 session 内累积逻辑，浮层加一行 caption 说明数据来源（服务端全天 / 本会话累积）。
- 异动页面提醒：脉搏行下方读 `load_pulse_alerts()`，最近 30 分钟内有异动则常驻一条 `st.warning`（如「⚡ 14:32 炸板潮：10 分钟 2 → 6」，多条取最新一条并注明「今日共 N 次异动」）；新异动首次出现在本会话时 `st.toast` 一次（session_state 记已见 alert 键去重，60s fragment 重跑不反复弹）。

## 5. 设计 C：爆量检测范围全开 + 口径动态展示

- **生效方式（代码零改动）**：云端 82.156.0.68（lighthouse 用户）`/home/lighthouse/rquant/.env` 加 `RQUANT_SURGE_BOARDS=all`，收盘后重启 `rquant-surge-watch.service`（或次日 09:25 timer 自然生效）。生产配置变更，由用户确认后执行或授权 Codex 操作。
- **口径动态展示**：surge-watch 启动时把生效配置（boards、k_rough、k_cum、ratio_cap、限频等）原子写 `surge_live/runtime_config.json`；爆量记录 tab 的 caption 从新 loader `load_surge_runtime_config()` 动态读取显示「检测范围：主板/创业/科创/北交」与口径参数，文件缺失时退回现在的写死文案。消除「为什么没收录」的困惑来源。
- **限频观察项**：全开后主板约 3100 只进入粗筛池，确认层 tushare 限频 2 只/分钟不动，先跑 1-2 天；若 events 里 `confirmed_at` 相对粗筛通过时刻明显滞后（排队证据），再评估调 `tushare_rate_per_min`。
- 8× 毒尾上限与方向门保持现状（回测验证过的刻意设计）。

## 6. 设计 D：量柱方向近似上色

- 分时/5日量柱颜色：该分钟收涨红（`#ef4444`）/ 收跌绿（`#10b981`）/ 平盘灰（`#94a3b8`），方向 = `price.diff()` 符号，首根无前值记灰。
- caption 追加「量柱颜色为分钟涨跌近似（tick-rule），非真实内外盘」。
- 日K 量柱已按涨跌上色，不动。

## 7. 数据契约汇总（新增文件均在 `settings.data_dir/surge_live/`）

| 文件 | 写者 | 读者 | 格式 |
|------|------|------|------|
| `pulse-YYYY-MM-DD.jsonl` | surge-watch（append） | 全景页 `load_pulse_history` | 每行一分钟脉搏计数 |
| `pulse_alerts-YYYY-MM-DD.jsonl` | surge-watch（append） | 全景页 `load_pulse_alerts` | 每行一次异动事件 |
| `runtime_config.json` | surge-watch（启动时原子写） | 全景页 `load_surge_runtime_config` | 当前生效口径 |
| `events-YYYY-MM-DD.jsonl`（已有） | surge-watch | 新增读者 `load_surge_marks` | 不变 |

全景页保持纯只读、绝不写库/写文件的纪律不变；所有 loader 对文件缺失/坏行做降级（空表/空值），不抛异常。

## 8. 测试与验证

- **单测**：`PulseAnomalyWatcher`（四类规则触发/不触发边界、冷却、重启 seed、开盘前 10 分钟窗口不足）；`load_pulse_history` / `load_pulse_alerts` / `load_surge_marks` / `load_surge_runtime_config`（缺失/坏行降级）；标记时刻 → bar 序号映射（精确命中、缺根回退、当天无数据跳过）；量柱方向列计算。
- **surge-watch 集成**：注入 fake 快照 + `force_session` 实跑若干 tick，验证 pulse / alerts / runtime_config 落盘与 dry-run 推送报文格式。
- **e2e（Playwright）**：扩 `RQUANT_PANORAMA_FAKE=1` fixture（fake 爆量台账 + fake 脉搏历史 + fake 异动 + fake 标记），覆盖：爆量 tab 选行出图带标记、浮层分面四小图、异动 banner 展示、爆量 tab 空数据 / 无标记边界。UI 改动必须自测通过再交付（既定约定）。
- 合并前本地实际运行全景页核心路径（合 main 硬规则）。

## 9. 交付与部署

1. 分支 `cc/pano-board-surge-pulse` → PR → Python 3.11/3.12 CI 全绿 → squash merge。
2. tag 预计 `v0.28.0`，`CHANGELOG.md` 更新 Added/Changed。
3. `bash scripts/deploy-production.sh --target v0.28.0` 部署云端（需要重启的发布在工作日 09:15-15:10 自动延期，实际执行在收盘后）。
4. 云端 `.env` 补 `RQUANT_SURGE_BOARDS=all` 并重启 `rquant-surge-watch.service`（用户确认后执行）。
5. `DEPLOY.md` 记录本次部署。

## 10. 非目标（本批不做）

- iTick VP / LC / 盘口深度看板（等证伪报告另立项，独立 canvas 页方案）。
- 8× 毒尾上限、方向门、限频参数的调整（保留观察项）。
- 漏报归因工具（根因已锁定为检测范围；若全开后仍出现困惑案例再立项）。
- 精确内外盘数据接入（依赖 iTick 立项结论）。
