# 盘中市场全景页面 + 板块资金流下钻 设计文档

**目标：** 盘中最快发现资金流入哪些板块，并能下钻到板块内个股，看每只成分股「分钟级 B 信号」多因子命中情况。同时提供分钟级涨停/跌停/炸板计数和板块成交额排序。

**定位：** 属于「条件筛选 + 实时监控 + 告警通知」中的监控范畴，不涉及下单 / 高频 / Tick，不越项目边界。

**架构原则：** 全景页是**纯只读消费者**——读 DuckDB 只走 `open_readonly_store()` 副本，盘中拉取的 rt_min 分钟数据只在页面进程内存里算因子、**绝不写主库**（monitor 是唯一常驻写者，页面写主库会撞写锁，参照 CLAUDE.md DuckDB 并发约束）。若分钟数据需要沉淀，由 monitor 或收盘后回补承担。

---

## 1. 数据源矩阵（已验证可用性）

| # | 源 | 接口 | 提供什么 | 最小安全轮询间隔 | 云端可用 |
|---|-----|------|---------|----------------|---------|
| S1 | akshare 新浪全市场快照 | `ak.stock_zh_a_spot()` | 全市场 ~5400 只：最新价/今开/最高/最低/昨收/涨跌幅/成交量/成交额，单次调用约 3-5 秒返回。monitor 的 `fetch_realtime_quotes` 在用 | 30s（monitor fallback 同源，页面独立进程再拉一路，30s 一次全量对 sina 安全；不要低于 15s） | ✅（sina 可用；东财 spot_em 云端被屏蔽） |
| S2 | akshare 东财板块资金流 | `ak.stock_sector_fund_flow_rank(indicator="今日", sector_type="行业资金流"/"概念资金流")` | 板块主力净流入额、净流入占比、涨跌幅、领涨股 | 60s（东财反爬敏感，列名可能漂移，参照 `limit_up_pool.py` 的缺列防御先例） | ❌ 腾讯云被屏蔽 → **页面只能本地跑** |
| S3 | akshare 东财板块行情 | `ak.stock_board_industry_name_em()` / `ak.stock_board_concept_name_em()` | 板块级最新价/涨跌幅/成交额/换手 | 60s | ❌ 本地 only |
| S4 | akshare 东财涨停/炸板/跌停池 | `ak.stock_zt_pool_em` / `ak.stock_zt_pool_zbgc_em` / `ak.stock_zt_pool_dtgc_em` | 涨停池（封单/首封时间/炸板次数/连板）、炸板池、跌停池，盘中实时 | 60s（normalize 代码可复用 `limit_up_pool.py`） | ❌ 本地 only |
| S5 | Tushare rt_min / rt_min_daily | `TushareAdapter.rt_min`（批量多码一次请求）/ `rt_min_daily`（单票当日全部分钟，逐票请求） | 付费分钟 K（¥1000/月已购），下钻按需调用 | 15s（`settings.rt_min_poll_seconds` 同款节流）；rt_min_daily 单票 ~0.2-0.5s/次，只对勾选票拉 | ✅ |
| L1 | 本地 daily_state / state.derive | `daily_state.limit_pct` + `_classify_board/_detect_st/_limit_pct/_round_half_up` | 今日涨停价 = round(昨收 × (1+limit_pct))，昨收来自 S1 快照的「昨收」列，limit_pct 由代码段+ST 名称判定（`state/derive.py` 现成函数） | 静态（开页算一次缓存） | — |
| L2 | 本地 ths_member / dc_member | 板块成分表（**即将接入**，设计按已有假设：`(index_code, ts_code)` 成分映射 + 板块名称表） | 正式板块体系（同花顺/东财概念+行业） | 静态（日更） | — |
| L3 | 本地 stock_basic.industry | 现有表 | 成分表没到位前的降级板块分组 | 静态 | — |
| L4 | 本地 auction_bar / minute_bar / market_sentiment_daily / pool2_watch / screen_result | 现有表 | 竞价因子输入、分钟历史基准、市场温度门控（昨日值）、池内标记 | 静态（只读副本，延迟 ≤5min 可接受） | — |

**页面刷新节奏：** 整页 `st.cache_data` 分层 TTL——S1 快照 30s、S2/S3/S4 东财 60s、L 系列 300s；页面 meta refresh 60s（比健康看板 30s 松，因为东财源是短板）。下钻的 rt_min 调用不进自动刷新循环，只由用户点击触发。

## 2. 页面信息架构

```
┌─ 顶部计数条（60s 刷新）───────────────────────────────────────┐
│ 涨停 N（首板 n1/连板 n2）  跌停 M   炸板 K   [当日分钟走势 sparkline] │
├─ 板块排行（双栏，60s 刷新）──────────────────────────────────┤
│ 左：资金流排行榜               右：成交额排行                    │
│  tab: 行业 | 概念               tab: 行业 | 概念                │
│  排序切换: 净流入额 | 净流入率    列: 成交额/涨跌幅/涨停家数        │
├─ 板块下钻（点击板块行展开）───────────────────────────────────┤
│ 成分股列表: 代码/名称/涨幅/成交额/换手/是否池内(pool1/pool2/持仓)   │
│ [勾选 ≤10 只 → 计算分钟级 B 信号] 按钮                          │
├─ B 信号多因子面板（按需计算）─────────────────────────────────┤
│ 因子命中矩阵: 每股一行 × 每因子一列，亮灯 ✅/❌/–(无数据) + 综合分  │
└──────────────────────────────────────────────────────────┘
```

### 2.1 各 UI 元素 → 数据源映射

| UI 元素 | 主源 | 计算方式 | 备注 |
|---------|------|---------|------|
| 涨停/跌停计数 | S1 + L1 | 快照最新价 ≥ 涨停价 − 0.01 计涨停（跌停对称）；涨停价由昨收+limit_pct 本地算 | 与 S4 东财池数对照展示，偏差大时页面亮黄 |
| 炸板计数 | S4 炸板池 | 直接取东财炸板池行数 | 降级方案：页面 session 内跟踪「曾涨停现回落」，但重启丢状态，S4 为主 |
| 分钟走势 sparkline | 页面内存 | 每轮刷新把 (时刻, 涨停数, 跌停数, 炸板数) append 进 `st.session_state`，Altair 画当日折线 | P0 不落库；要跨进程持久化等 P2 让 monitor 写研究表 |
| 板块资金流排行 | S2 | 净流入额 / 净流入率 双排序切换，行业与概念两个 tab | 云端不可用 → 页面标注「仅本地」 |
| 板块成交额排行 | S3；降级 S1+L3 | 降级方案：快照按 stock_basic.industry groupby SUM(成交额)、MEDIAN(涨跌幅)、涨停家数 | P0 用降级方案即可上线 |
| 成分股列表 | L2（P1）/ L3（P0）+ S1 | 板块 → 成分码 → 从快照 dict 取行情 | 池内标记 join `pool2_watch(active)` + 当日 `screen_result` + `paper_position(open)` |
| B 信号因子面板 | S5 + L4 | 见第 3 节，快照档因子免费即时、分钟档因子按需 rt_min | 勾选 ≤10 只硬上限，控预算 |

## 3. B 信号多因子面板

### 3.1 因子清单（盘点自现有代码，全部为入场时点可算、无未来函数）

分两档：**快照档**（S1 快照字段就够，全板块即时、零成本）和**分钟档**（需要当日分钟线，rt_min 按需）。

| 档 | 因子 | 现有实现出处 | 输入 | 盘中可算性 |
|----|------|------------|------|-----------|
| 快照 | 竞价跳空幅度 `gap_pct_close`（竞价价/昨收−1） | `auction_gap_strategy.run_auction_gap_replay` | auction_bar 当日 `open_realtime`（stk_auction 9:26 后可取）；无当日竞价数据时降级用快照「今开」近似 | ✅ |
| 快照 | 竞价量比 `auction_vol_ratio_5d`（竞价量/5日均量） | 同上（`avg_vol_5d` 滚动） | auction_bar + daily_bar | ✅ |
| 快照 | 竞价成交额 / 换手率 | 同上 | auction_bar | ✅ |
| 快照 | 距涨停空间 `entry_to_limit_up_pct` | 同上 | 快照最新价 + L1 涨停价 | ✅ |
| 快照 | VWAP 位置（价格 ≥ 当日均价） | `auction_gap_strategy._find_auction_gap_entry` 的 cum_amount/cum_vol；**快照近似**：VWAP = 快照成交额/成交量 | S1 快照 | ✅ 快照即全日累计，无需分钟 |
| 快照 | 涨停进度 `limit_progress`（(日内高−今开)/(涨停价−今开)） | 同上（anchor 为竞价价=今开） | S1 快照「最高」「今开」+ L1 | ✅ |
| 快照 | 回撤承接 `support_ok`（日内低 ≥ 今开 × (1−2%)） | 同上 | S1 快照「最低」「今开」 | ✅ |
| 快照 | 市场温度门控（昨日 `high_60d_ratio_pct` / `above_ma20_ratio_pct` / `limit_up_count`） | `market_context.MarketSentiment` | market_sentiment_daily 昨日行（无未来函数原则：日级数据只用前一交易日） | ✅ 全局一份 |
| 快照 | 日线位置/吸筹慢因子（`price_position_90d`、`ma_alignment`、`accum_obv_change_20d` 等，可选列） | `stock_features.build_daily_stock_features` | daily_bar 截至昨收 | ✅ 但单票 ~10ms×N，只对下钻板块算 |
| 分钟 | 同分钟相对放量 `signal_rel_amount_same_minute_20d` / 累计额进度 `signal_rel_cum_amount_asof_20d` | `stock_features.build_intraday_relative_volume_features` | minute_bar 历史 20 日同分钟基准 + 当日分钟（rt_min_daily） | ⚠️ **历史 minute_bar 只回补过候选票**；板块内任意票多数无历史基准 → 显示「–（无基准）」灰灯，不计入综合分分母 |
| 分钟 | 分钟放量加速 `signal_amount_accel_5m/10m`（当前分钟额/此前 5/10 分钟中位数） | 同上 | 仅当日分钟（rt_min_daily），**无需历史** | ✅ |
| 分钟 | 封板强度组 `b_open_times`（开板次数）/`b_limit_up_touch_minutes`/`b_close_at_limit_up` | `auction_gap_strategy._b_day_strength` | 当日分钟 | ✅ 仅对触板票有意义 |
| 分钟 | N 字上攻信号组 `attack_open_strength/strong_carry/break_high/near_limit` | `monitor.check_attack_signals` | 需要 T 日参照（t_close/t_high），**仅池内票适用**，非池票该列显示「–」 | ✅ 池内 |

### 3.2 因子命中矩阵展示

- 每股一行、每因子一列；单元格三态：✅ 命中（绿）/ ❌ 未命中（灰）/ –（无数据，不计分）。
- 综合分 = 命中数 / 可算因子数（等权计数）。**不做加权拟合**——权重优化属于 Lab 归因的活（竞价跳空归因已判死原始策略，因子权重必须走 walk-forward，不在监控页拍脑袋）。
- hover/展开显示因子原始值与阈值（如 `rel_amount_same_minute_20d = 3.1 ≥ 2.0`）。
- 阈值集中定义在一个 `FactorSpec` 清单（pydantic 模型：name/label/threshold/op/tier），与文档二的 `signal_factors` JSON 结构共用同一份 spec，保证监控页看到的因子和模拟盘落库的因子同名同义。

## 4. 工程结构

### 4.1 新独立 Streamlit 页

- 入口：`src/rquant/dashboard/market_panorama.py`，**端口 8505**（8501 健康 / 8502 nl_screen / 8503 nl_canvas / 8504 Lab 已占用）。
- 与现有页面关系：健康看板管「系统好不好」，Lab 管「策略研究」，全景页管「今天市场哪里热」。互不 import 页面级代码，共享数据层模块。
- 启动方式写在文件头 docstring（对齐 app.py / strategy_lab.py 惯例）；**不进云端 systemd**（东财源云端被屏蔽），只在本地 mac 手动/launchd 起。

### 4.2 后端数据获取层

```
src/rquant/sector_flow.py          # akshare 东财板块资金流/板块行情 fetcher
                                   # 纯函数 + normalize + 缺列防御，风格对齐 limit_up_pool.py
src/rquant/dashboard/market_panorama_data.py
                                   # 页面数据编排：计数条、板块聚合、下钻查询、
                                   # 因子矩阵计算（调 stock_features / auction_gap 公共函数）
src/rquant/intraday_factors.py     # (P2) 从 auction_gap_strategy._find_auction_gap_entry
                                   # 抽出的逐分钟状态计算 + FactorSpec 清单，
                                   # 供全景页 / replay / 未来 live paper 三方复用
```

- **复用 monitor 的模块级函数、不复用 provider 实例**：`fetch_realtime_quotes` 是无状态函数，页面直接 import 调用；`IntradayMinuteQuoteProvider` 绑定了写库 store，页面进程自建一个 `store=None` 的轻量实例（或直接调 `TushareAdapter.rt_min/rt_min_daily`），拉到的分钟数据只留内存。
- 页面所有 DuckDB 查询走 `open_readonly_store()` / `open_readonly_connection()`，写锁冲突时降级提示（对齐 strategy_lab.py 的 `query_duckdb` 模式）。

### 4.3 性能预算（下钻单板块 30-100 只）

| 操作 | 请求数 | 耗时估计 | 结论 |
|------|-------|---------|------|
| 快照档因子（全板块） | 0（复用 30s 缓存的全市场快照） | <0.1s pandas 过滤 | 免费即时 |
| 竞价因子（全板块） | 0（auction_bar 当日已由回补/monitor 落库；缺则降级今开） | <0.1s | 免费 |
| 慢因子 build_daily_stock_features | 0 | 100 只 × ~10ms ≈ 1s | 可整板块算，加 spinner |
| rt_min 批量最新分钟（全板块） | 1-2 次（批量多码） | ~1s | 可行但增量信息少（快照已含价量），P2 再评估 |
| rt_min_daily 当日全部分钟（勾选票） | 每票 1 次 | 10 票 × 0.3s ≈ 3s | **硬上限 10 只**，超出让用户分批 |
| 分钟档因子计算 | 0 | 单票 8.6ms 量级（roadmap 3.2 实测） | 忽略 |

**rt_min 调用预算**：monitor 盘中 15s 节流常驻消耗为主；全景页下钻是人工触发、单次 ≤12 请求，日常几十次点击对 ¥1000/月 配额影响可忽略。页面侧对 rt_min_daily 加 60s 结果缓存（同一票 1 分钟内重复点击不重复请求）。

## 5. 分期交付

### P0 — 快照全景（不依赖板块成分表）

功能：涨停/跌停/炸板计数条（含东财池对照 + sparkline）、板块资金流排行（S2）、板块成交额粗排序（S1 快照 groupby stock_basic.industry 降级方案）、粗板块下钻成分列表（industry 分组 + 池内标记）。

| 改动文件 | 内容 |
|---------|------|
| `src/rquant/sector_flow.py`（新） | 东财板块资金流/行情/涨停池三组 fetcher + normalize + 列防御 |
| `src/rquant/dashboard/market_panorama.py`（新） | 页面骨架：计数条 + 双排行 + industry 下钻 |
| `src/rquant/dashboard/market_panorama_data.py`（新） | 涨停价计算（复用 state/derive）、计数逻辑、板块聚合、池内标记查询 |
| `tests/unit/test_sector_flow.py`（新） | normalize 与缺列防御单测 |

工作量：**2 天**（东财接口字段核验 + 页面 1.5 天，测试 0.5 天）。

### P1 — 正式板块体系（ths_member / dc_member 接入后）

前置依赖：板块成分 ingest 线交付 `ths_member` / `dc_member` 表（若届时未交付，本期顺带做：新表 DDL + Tushare `ths_index/ths_member`、`dc_index/dc_member` 日更回补 CLI）。

功能：下钻从 industry 切到同花顺/东财板块体系；板块资金流排行与本地成分对齐（点资金流榜任意板块都能钻）；概念板块支持。

| 改动文件 | 内容 |
|---------|------|
| `src/rquant/storage/schema.py` | `sector_member` 相关 DDL（若 ingest 线未覆盖） |
| `src/rquant/dashboard/market_panorama_data.py` | 下钻查询改走成分表，industry 保留为 fallback |
| `src/rquant/dashboard/market_panorama.py` | 板块体系切换（ths/dc/industry） |

工作量：**1-2 天**（含成分表名称对齐东财资金流榜的板块名 fuzzy match，这一步最容易脏）。

### P2 — 下钻 B 信号多因子面板

功能：因子命中矩阵组件；快照档因子全板块即时算；分钟档因子勾选 ≤10 只按需 rt_min_daily；综合分排序。

| 改动文件 | 内容 |
|---------|------|
| `src/rquant/intraday_factors.py`（新） | FactorSpec 清单 + 从 `_find_auction_gap_entry` 抽出的逐分钟状态计算（重构不改 replay 行为，原函数改调新模块） |
| `src/rquant/auction_gap_strategy.py` | 入场扫描改用 intraday_factors 公共实现（行为等价，回归测试护航） |
| `src/rquant/dashboard/market_panorama.py` / `market_panorama_data.py` | 因子矩阵 UI + rt_min 按需拉取与缓存 |
| `tests/unit/test_intraday_factors.py`（新） | 因子计算与 replay 原实现对拍 |

工作量：**2-3 天**（重构对拍 1 天、UI 1 天、rt_min 集成 0.5-1 天）。

## 6. 风险与边界

1. **akshare 东财限频/反爬**：列名漂移、UA 封禁都发生过；所有东财 fetcher 必须走「缺列 → log + 返回空 → 页面降级」路径（limit_up_pool.py 先例），页面对空数据显示「源暂不可用」而非报错。
2. **云端不可部署**：S2/S3/S4 腾讯云被屏蔽，页面明确标注「仅本地运行」；不要把全景页加进 deploy/systemd。若未来想云端跑，需评估申万实时行情（¥200/月，roadmap 暂缓项）替代东财板块源。
3. **rt_min 预算**：¥1000/月已购，monitor 常驻消耗为主；页面下钻人工触发 + 10 只硬上限 + 60s 缓存，三重限制下预算无忧。若 Tushare 单分钟请求数告警，先砍 rt_min 批量全板块那一档（信息增量最小）。
4. **DuckDB 锁纪律**：页面只读副本；rt_min 拉的数据不落库。code review 必查（CLAUDE.md 强制项）。
5. **历史分钟基准缺口**：同分钟相对放量因子对未回补过的票无基准，矩阵显示灰灯即可，**不要**为了补基准盘中批量拉 stk_mins（历史接口盘中调用又慢又占配额）。
6. **边界确认**：只做监控展示与人工下钻判断辅助，不自动触发买入（自动模拟买入是文档二的范畴，且也只是模拟盘）。
