# Changelog

本项目遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/) 格式，版本号遵循 [SemVer](https://semver.org/lang/zh-CN/)。

## [Unreleased]

---

## [v0.7.0] — 2026-04-29 — 云端部署 + 多通道通知 + Health Dashboard

把 rQuant 从本地 macOS 单点搬到腾讯云轻量服务器（82.156.0.68）systemd 调度，
解决本地笔记本休眠 APScheduler 死亡问题。增加 PushPlus 通道（不装 PushDeer
的协作者）、Streamlit Health Dashboard、本地热备 rsync 同步。

### Added
- systemd timer + service（`deploy/systemd/`）：daily 17:00 + monitor 09:25
  工作日触发，腾讯云 OpenCloudOS 9 验证通过
- PushPlus 通道（`notify/client.py:PushPlusClient`）：微信公众号推送，给
  不装 PushDeer 的用户（如美丞）；与 PushDeer 双通道独立失败
- Health Dashboard（`src/rquant/dashboard/app.py`）：Streamlit 单页 9 个指标
  - systemd 服务状态 / Watchlist / 今日触发事件 / 数据新鲜度 / 7 日趋势
  - 通知通道 24h 成功率 / 本地 sync 状态 / Pool 2 实时价位 vs 档位
  - Pool 2 行点击下钻：日 K candlestick + 分时（午休 11:30-13:00 跳过空段）
  - 30s 自动刷新 + Linear/Vercel 风格紧凑 UI
- 本地热备 rsync（`scripts/sync-from-cloud.sh` + `deploy/com.roxor.rquant-sync.plist`）：
  - 盘中 09:30-15:05 + 日终 17:10-17:30 同步窗口
  - rsync `--delay-updates` 原子 rename 保证 mac 端读到完整文件
  - `--force` 选项手动触发；失败 PushDeer 告警
- `notification_log` 表 + `notify/api.py` 写入推送日志（dashboard 读取展示）
- `_to_sina_symbol` helper：ts_code → sina 代码格式（sh/sz/bj 前缀）
- CLAUDE.md 新增"生产环境与协作模式"小节：服务器 IP / Hybrid 协作分工 /
  通知通道分工
- `deploy/dashboard.md` + `deploy/local-sync.md` + `deploy/systemd/README.md`：
  部署 + nginx basic auth + 故障排查文档

### Fixed
- monitor `fetch_realtime_prices` 从 `stock_zh_a_spot_em`（东方财富，云端
  腾讯云 IP 段被屏蔽）改 `stock_zh_a_spot`（sina HQ 接口），云端可用
- dashboard K 线 / 分时 API 同步换 sina：`stock_zh_a_daily` +
  `stock_zh_a_minute` 替代东方财富版本
- dashboard DuckDB 写锁冲突优雅降级：query 返回 None 时 UI 显示等待提示，
  不再裸抛错误堆栈（daily 流水线 ingest 期间 dashboard 自动降级）
- dashboard UI 大幅紧凑化：字号 16→13px、metric 卡值 1.5→1.15rem、H2
  改 Vercel 小号大写、container border 1px 浅灰圆角、健康 badge 扁平化
- 分时图 11:30-13:00 午休空段：x 轴改 ordinal 跳过空段，加灰虚线分隔
- dashboard Pool 2 实时价位：sina HQ 批量接口替代 ak.stock_zh_a_spot 全市场
  拉取（300ms vs 3s），数字列严格 `%.2f` 格式
- sync 窗口策略修正：原"每小时跑 + 业务时段跳过"改为"仅业务时段相关跑 +
  其他时间不跑"，避免错过 monitor_event 实时备份
- systemd timer NEXT 字段微秒时间戳转 UTC+8 + delta 显示"X 小时后"

### Changed
- `WatchItem` 加 `name` / `entry_date` 字段，`build_watchlist` 末尾批量
  从 `stock_basic` join 填股票名（dashboard 用）
- `_send_pushdeer` / `_send_pushplus` 写 `notification_log` 表记录每条
  推送的 target/success/error_msg
- `pyproject.toml` 新增 streamlit 依赖

---

## [v0.6.0] — 2026-04-29 — Week 6: PushDeer 告警通知

替换原计划的 cc2im（受限于微信 token 限制）为 PushDeer。完全替代 monitor.py 的 osascript 弹窗，云端零迁移成本。

### Added
- `notify` 独立模块（`src/rquant/notify/`）：
  - `client.py` — PushDeerClient，多 key 并发推，timeout/异常都捕获不抛
  - `messages.py` — 5 类场景消息构造（price_level / pool2_exit / daily_summary / error / heartbeat）
  - `api.py` — `notify(scene, **kwargs)` 统一入口 + 总开关 + 各场景独立开关
- `rquant notify-test` CLI 命令：直接推 PushDeer 测试消息验证通道
- 5 类推送场景接入：
  - **A 档位触发**：实时单条（替换 osascript 弹窗），价格阶梯从高到低展示（bodyTop / 40 / 30 / 20 / bodyBtm + 强弱止）
  - **B Pool 2 退出汇总**：收盘后批量一条（无事件不推），breakdown 自动踢，expired 保留待用户决策
  - **C 每日筛选汇总**：17:00 流水线完成后一条，含 Pool 1 命中名单 + Pool 2 持仓状态 + 耗时
  - **D 系统异常**：cli/pipeline/monitor 入口 try/except 捕获后实时推（含 stack trace 前 15 行）
  - **E Monitor 启停心跳**：09:30 启动 + 15:00 结束各一条
- `WatchItem` 新增 `name` 和 `entry_date` 字段，`build_watchlist` 末尾批量从 `stock_basic` join 填股票名
- `tests/conftest.py` autouse fixture：默认禁用真实 PushDeer 推送，避免测试副作用刷手机
- 配置项 `.env`/`.env.example`：`PUSHDEER_KEYS` / `PUSHDEER_ENDPOINT` / `NOTIFY_*` 开关

### Changed
- `monitor.check_exits()` 改为自动化：breakdown 直接 `update_pool2_exit`，expired 保留 active 加入待决策列表，末尾推汇总，返回 `auto_kicked_count` 用于心跳统计
- `pipeline.run_daily_pipeline()` 末尾计算耗时并触发 daily_summary 推送

### Removed
- `monitor.alert_price_level()`（osascript 弹窗，被 PushDeer 替代）
- `monitor.alert_exit_confirm()`（osascript 退出确认弹窗，PushDeer 单向推无法承载交互决策）
- `subprocess` 导入（不再使用）

### Fixed
- 测试套件：删除 osascript 相关测试用例（TestAlertPriceLevel / TestAlertExitConfirm），新增 32 个 notify 模块测试 + check_exits 重写后的 3 个测试

---

## [v0.5.1] — 2026-04-28 — Hotfixes: 调度可靠性 + monitor 自动拉起

### Fixed
- `rquant serve` APScheduler 可靠性：`misfire_grace_time` 从 1s → 3600s → 7200s（覆盖周末/长 sleep 后的 misfire），并加 stdlib logging bridge 输出 APS 内部错误
- monitor 自动每日拉起：`com.roxor.rquant-monitor.plist` 加 `StartCalendarInterval` 09:29，`run_monitor` 加 `_wait_for_market_open()` 在 09:30 前 10 分钟内 sleep 到开盘

---

## [v0.5.0] — 2026-04-21 — Week 5a: 盘中实时监控 + Pool 2 持久池

### Added
- `rquant monitor` 命令：盘中实时监控 Pool 1 + Pool 2 标的价格
  - akshare 实时行情轮询（5 秒间隔），检测 5 个档位（40%/30%/20%/强止/弱止）
  - macOS 原生弹窗提醒（osascript display alert），非阻塞
  - 当日最低价补漏机制，防止闪跌遗漏
  - 交易日历检查（含中国节假日），非交易日自动跳过
- `pool2_watch` 表：Pool 2 持久池，从每日快照升级为有进出机制的持久池子
  - 入池：pipeline 跑完 Pool 2 筛选后自动同步
  - 退出：收盘后检查跌破止损/超期（3 天），所有退出弹窗确认（踢出/保留）
- `monitor_event` 表：盘中事件日志，记录每次档位触发详情
- `rquant pool2 list / remove` 命令：查看和管理持久池
- `deploy/com.roxor.rquant-monitor.plist`：盘中监控 launchd 自启配置
- `rquant ingest --date` 命令：按 trade_date 模式拉全市场 stock_basic + daily_bar + daily_basic + derive_state，约 30 秒完成
- `rquant run-daily` 现在自动先 ingest 再 pipeline（`--no-ingest` 跳过）
- `rquant serve` 的 cron 改为 ingest → pipeline 串联，数据未就绪时自动重试 3 次（间隔 15 分钟）
- `deploy/com.roxor.rquant.plist`：macOS launchd 开机自启配置

### Changed
- `pipeline.py`：`run_daily_pipeline()` 尾部新增 pool2_watch 同步逻辑
- Pool 1 下影线阈值从 1.5 放宽至 0.5（下影/实体比），命中从 5 只提升至 12 只
- Pool 1 前涨停窗口从 90 交易日放宽至 120 交易日
- Pool 2 `offset_days` 从 1 改为 2，合并 T-1 + T-2 两天的父预设白名单
- Pool 2 下影线阈值同步从 1.5 放宽至 0.5
- `run_daily_pipeline()` 依赖链改为范围回溯：`offset_days=N` 表示合并 T-1 到 T-N 的父预设结果

---

## [v0.4.0] — 2026-04-20 — Week 4b: 调度 + 流水线 + N 形态预设

CLI 入口、APScheduler 调度、screen_result 落库、N 形态 Pool 1 + Pool 2 预设注册表、流水线依赖链编排。

### Added
- CLI：`rquant serve`（APScheduler cron，Mon-Fri 17:00）和 `rquant run-daily --date --preset` 子命令
- `screen_result` 表：筛选命中结果落库（trade_date + preset_name + ts_code，extra JSON 列存附加字段）
- `ScreenPreset` 数据类 + `PRESET_SCREENS` 注册表：Python 代码即策略声明，支持 depends_on 依赖链
- N 形态预设：Pool 1（11 条规则，全市场）+ Pool 2（3 条规则，依赖 Pool 1 T-1 结果子集）
- `run_daily_pipeline()`：按依赖拓扑排序遍历预设，子预设自动从父预设结果取 whitelist
- `screen()` 新增 `ts_code_whitelist` 参数，支持在指定子集中筛选

### Changed
- `pyproject.toml`：新增 `apscheduler>=3.10` 依赖 + `[project.scripts]` 入口

---

## [v0.3.1] — 2026-04-20 — Week 4a: daily_basic + N 形态积木

为 N 形态策略补全数据层和规则积木。新增 `daily_basic` 表接入流通市值/换手率/量比，宽表暴露 `BODY_UPPER[n]`/`BODY_LOWER[n]`/`CIRC_MV[n]`，6 个新积木 + AggregateRequest 长窗口聚合机制。

### Added
- `daily_basic` 表（turnover_rate / volume_ratio / total_mv / circ_mv）
  - `DuckDBStore.upsert_daily_basic()` / `count_daily_basic()`
  - `TushareAdapter.daily_basic(ts_codes, trade_date)` — 单日查询
  - `ingest_daily.py` 追加按日逐天拉取 daily_basic
- 宽表扩展：
  - `STATE_COLS_MAP` 新增 body_upper / body_lower → `BODY_UPPER[n]` / `BODY_LOWER[n]`
  - 新增 `BASIC_COLS_MAP`（circ_mv / total_mv / turnover_rate）→ `CIRC_MV[n]` / `TOTAL_MV[n]` / `TURNOVER_RATE[n]`
- AggregateRequest 机制：规则声明长窗口聚合需求（max / any / sum / count_nonzero），load_universe 动态生成 DuckDB SQL，支持 exclude_offset
- 6 个新积木：
  - `not_yiziban(offset)` — 某日非一字板
  - `circ_mv_lt(threshold_yi, offset)` — 流通市值 < N 亿
  - `has_lower_shadow(min_ratio, min_amplitude, offset)` — 下影线达标
  - `no_consec_ups_in_window(threshold, window)` — 近 N 日无 M 连板
  - `no_limit_down_in_window(window)` — 近 N 日无跌停
  - `has_prior_limit_up(window, exclude_offset)` — 近 N 日（排除某日）有涨停
- 测试：新增 ~50 个单测（storage 4 + loader 11 + rules 30+ + core 4），累计 162 个

---

## [v0.3.0] — 2026-04-16 — Week 3b: 筛选规则引擎

Week 3b 在 daily_state + daily_indicator 基础上做多条件组合筛选。原子条件"积木"函数库，命名对齐通达信/MyTT 风格（`CLOSE[0]` / `MA20[0]` / `IS_LIMIT_UP[1]`），为 Week 8 通达信代码支持铺路。

### Added
- `rquant.screen` package：
  - `load_universe(trade_date, lookback)`：从 DuckDB 加载全市场宽表（每行 1 只股票，字段 `CLOSE[n]` / `MA20[n]` / `IS_LIMIT_UP[n]` 等）
  - 积木函数库：属性（not_st / not_bj / board_in）、涨跌停（limit_up / first_limit_up / yiziban / consecutive_ups_gte / limit_down / not_limit_up）、比较（gt / lt / gte / lte / between）、指标（cross_above / cross_below / above_ma / rsi_oversold / rsi_overbought）、成交量（volume_ratio_gte）
  - `screen(trade_date, rules)`：AND 组合 + 自动 lookback 推断 + 结果 DataFrame 返回
- `scripts/smoke_screen.py`：跑用户原始场景的冒烟脚本

### Verified
- 用户原始场景「非 ST + 非北交所 + 昨首板 + 今未涨停 + 今高>昨收」在集成测试 + 真实近期数据（2026-04-15 前后）上跑通
- 单测：新增约 30 条（属性 6 + 涨跌停 7 + 比较 6 + 指标 5 + 成交量 1 + screen 5 + loader 5），全量 105 绿，整体累积 ~165 个

---

## [v0.2.1] — 2026-04-16 — Week 3a: 派生字段层（daily_state）

为 Week 3b 筛选规则引擎铺底：把「涨停/跌停/首板/一字板/连板/实体上下沿/板块/ST」这些 SQL 难表达的概念先算好落库，规则引擎只做 SELECT 过滤。

### Added
- `rquant.state.derive` 模块：基于日线原始价（非前复权，涨停判断必须用真 `pre_close`）推导 15 列派生字段
  - `_classify_board(ts_code)`：688/689 → star，300/301 → gem，.BJ → bj，else main
  - `_detect_st(name)`：忽略空格，识别 `ST` / `*ST` / `SST` 前缀
  - `_limit_pct(is_st, board_type)`：ST 5% / 主板 10% / 创业板科创板 20% / 北交所 30%
  - `derive_state(df_daily, ts_code, name)`：一次算完 `is_limit_up` / `is_first_limit_up` / `is_yiziban` / `consecutive_limit_ups` / `body_upper` / `body_lower`
  - 涨停识别带 1 分价格容差（`close >= limit_up_price - 0.01`）
- `schema.DAILY_STATE_DDL`：`daily_state` 表（15 列，PK = ts_code + trade_date）
- `DuckDBStore.upsert_state` / `count_state` / `get_state`
- 依赖：`mytt==2.9.3`（通达信/同花顺风格公式库，用其 `BARSLASTCOUNT` 算连板数）
- `ingest_daily.py` 扩展：拉完 daily 后自动算派生字段落 `daily_state`；顺带拉一次全量 `stock_basic`（~5500 行）用于 ST 判断
- `status.py` 扩展：展示每只股票的涨停/跌停/首板/一字板/最大连板统计，以及最新一日的涨跌停价和实体区间
- 测试：新增 42 个单测（state 模块 37 个 + storage 5 个），累计 65 个全部通过

### Verified
- 赛力斯 601127.SH（华为概念股）2024-09-30 / 10-23 / 11-04 / 11-05 四次涨停识别正确，11-04+11-05 连板 2 正确，首板标记正确
- 涨停价公式：`pre_close × (1 + limit_pct)` 对主板 10% / 创业板 20% 实测与东方财富一致
- 宁德时代 2024-10-08 +18.70% 因不足 20% 限制 → `is_limit_up=False` 正确

---

## [v0.2.0] — 2026-04-16 — Week 2: 复权因子 + 技术指标

### Added
- 复权因子层
  - `schema.ADJ_FACTOR_DDL`：`adj_factor` 表
  - `TushareAdapter.adj_factor(ts_codes, start, end)`
  - `DuckDBStore.upsert_adj_factor` / `get_daily_qfq(ts_code, start, end)` / `count_adj_factor`
  - `get_daily_qfq` 以该股票最新 adj_factor 为参照计算前复权价，同时返回原始价和因子值便于核验
- 技术指标层（基于前复权价，避免分红除权造成指标跳变）
  - `rquant.indicator.compute_indicators(df)`：MA5/10/20/60 + RSI6/14 + MACD(12,26,9) + KDJ(9,3,3)
  - KDJ 用 A 股常用口径（α=1/3 指数平滑）手写实现
  - `schema.DAILY_INDICATOR_DDL`：`daily_indicator` 表（13 列指标）
  - `DuckDBStore.upsert_indicators` / `count_indicators`
- `ingest_daily.py` 扩展：拉完 daily+factor 后自动基于全量 qfq 重算指标并入库
- `status.py` 扩展：展示首日/最新日的原始价 vs 前复权价对比、最新技术指标（含多空/金死叉判断）
- 依赖：`ta==0.11.0`（放弃 pandas-ta 因其锁死旧版 numba 与 pandas 3 冲突）
- 测试：新增 15 个单测（adj_factor/qfq 5 个 + 指标 8 个 + indicator 落库 2 个），累计 23 个全部通过

### Infrastructure
- Week 1 代码推送到 GitHub private：<https://github.com/roxorlt/rquant>

### Verified
- 茅台 2025-12-18 前复权收盘 1407.04（除权日 2025-12-19 前一天）对上东方财富
- 茅台 2026-04-15 MA5 = 1454.43 对上东方财富日 K 均线

---

## [v0.1.0] — 2026-04-16 — Week 1: 数据接入 + DuckDB 存储

### Added
- 项目 scaffold：uv 包管理 + Python 3.12 + pyproject.toml + ruff/pytest 配置
- 配置层：`rquant.config.Settings`（Pydantic Settings 读 `.env`，校验 token 长度、自动创建目录）
- 日志层：`rquant.logging.setup_logging`（loguru stderr + 按日轮转到 `logs/`，保留 30 天）
- 数据模型：`rquant.models.DailyBar`（Pydantic，frozen）
- Tushare Adapter：`rquant.adapter.TushareAdapter`
  - `daily(ts_codes, start, end)` 拉日线 OHLCV，主 token 失败自动切备用
  - `stock_basic(list_status)` 拉股票基础信息
- DuckDB 存储：`rquant.storage.DuckDBStore`
  - 建表 DDL 集中在 `schema.py`（`daily_bar` + `stock_basic`）
  - `upsert_daily` / `upsert_stock_basic` 幂等写入
  - context manager 支持
- CLI：`scripts/ingest_daily.py` 一次性拉历史日线入库
- 测试：8 个单测（config 4 + DuckDB 4），`tests/README.md` 规范说明

### Infrastructure
- 项目初始化：README + CLAUDE.md + docs/（data-sources-matrix、references）
- CHANGELOG.md（Keep a Changelog 格式）+ .gitignore（Python + data/ + .env）
- .env.example 模板，`.env` 忽略提交
- git init + `v0.0.1` scaffold tag

---

## 版本计划（MVP 路径对应）

| 版本 | 里程碑 | 对应周 |
|------|-------|--------|
| v0.1.0 | 数据接入 + DuckDB 存储跑通 | Week 1 |
| v0.2.0 | 指标计算模块 | Week 2 |
| v0.3.0 | 筛选规则引擎 | Week 3 |
| v0.4.0 | APScheduler 调度 | Week 4 |
| v0.5.0 | 盘中 Ashare 轮询监控 | Week 5 |
| v0.6.0 | cc2im 告警通知 | Week 6 |
| v0.7.0 | Streamlit 最小 UI（MVP 完整） | Week 7 |
| v1.0.0 | 第一次真正日常可用 | MVP 稳定后 |

---

<!--
模板用法：
- 每次合 main 前，把 [Unreleased] 下的条目整理好
- 打 tag 时，把 [Unreleased] 换成 [v0.X.0] - YYYY-MM-DD
- 下方重建一个空的 [Unreleased]
- 分类用：Added / Changed / Deprecated / Removed / Fixed / Security
-->
