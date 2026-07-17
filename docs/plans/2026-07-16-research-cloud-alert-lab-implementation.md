# rQuant 研究云化、告警治理与 Strategy Lab 重构 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 将研究数据权威从 Mac 安全迁到云端，收口盘中服务和告警权威，并把 Strategy Lab 改造成按需执行、可复现、可比较、可晋级模拟盘的统一研究工作台。

**Architecture:** 保留生产 DuckDB 处理日线、池子和盘中事件；新增独立研究 DuckDB 保存数据目录、覆盖率、特征、实验和任务状态；历史分钟与竞价按交易日写入分区 Parquet，当前先落腾讯云磁盘，达到容量阈值后无缝复制到 COS。云端负责持续采集、轻量回放、看板和告警，Mac 降为可删除的研究缓存与可选重计算 worker。

**Tech Stack:** Python 3.11/3.12、DuckDB、Parquet、SQLite WAL、Tushare `stk_mins` / `rt_min` / `rt_min_daily` / 集合竞价、systemd、Streamlit、Pydantic、pytest。

---

## 1. 已确认事实与架构决策

### 1.1 当前容量

- Mac `rquant.duckdb` 约 4.8GB，包含约 1,911 万行分钟线和约 194 万行竞价数据。
- Mac 当前只读副本约 4.8GB；2026-06-26 的 1GB 陈旧副本备份已安全删除。
- 腾讯云为 2 vCPU、7.5GiB 内存，当前约 74GB 可用；rQuant 数据约 559MB、备份约 758MB。
- 现有容量足以迁移存量及 pool/候选增量，不适合无限保存全市场每分钟数据。

### 1.2 数据源与 IP 结论

- Tushare `stk_mins`、`rt_min`、`rt_min_daily` 和竞价接口均为 token 鉴权，当前腾讯云
  `rt_min` 已真实成功运行，不要求 Mac IP。
- AKShare 的新浪/东方财富网页源可能对腾讯云机房 IP 触发反爬或断连。它们只能作为降级源，
  不能再成为生产主链路。2026-07-07 已将 surge 主源切到 Tushare `rt_min`。
- 核心 N 字、集合竞价和科创/创业放量策略没有必须留在 Mac 的数据接口。只有尚无正式接口
  替代的网页抓取实验才保留 Mac 采集代理，而且必须显式标为非权威降级数据。

### 1.3 最终数据布局

```text
/home/lighthouse/rquant/data/
  rquant.duckdb                 # 生产写库：日线、池子、monitor、通知业务状态
  rquant_ro.duckdb              # 生产只读副本
  research.duckdb               # 研究目录、覆盖率、特征、实验、任务与晋级状态
  research_ro.duckdb            # Lab/报表只读副本
  lake/
    minute/freq=1min/year=YYYY/month=MM/trade_date=YYYY-MM-DD/*.parquet
    auction/year=YYYY/month=MM/trade_date=YYYY-MM-DD/*.parquet
    snapshots/dataset_id=.../*.json
```

Parquet 分区是历史原始数据权威，`research.duckdb` 是元数据与派生状态权威。不得把远程
DuckDB 文件通过网络文件系统直接打开，也不得让回测 worker 写生产 DuckDB。

### 1.4 计算部署

- 云端保留：daily、monitor、surge-watch、分钟/竞价增量、模拟盘、通知、看板和轻量回放。
- 云端 2 vCPU 只允许一个轻量研究 worker，并通过 `nice`/systemd CPUQuota 在盘后运行。
- 大规模自动优化先由 Mac worker 读取云端 Parquet 缓存；若希望完全云化，再增加独立 4-8
  vCPU 研究实例，不能让优化器和盘中 monitor 共用 2 vCPU。
- Strategy Lab UI 可迁云供手机访问，但计算任务必须进队列，Streamlit 进程本身不执行长任务。

## 2. 发布批次与阶段门

| PR | 内容 | 阶段门 |
|---|---|---|
| A | Push 持久去重、单一告警权威、重启熔断 | 故障注入 10 次只收到 1 条普通告警，critical 最多 1 条 |
| B | 研究数据湖 schema、导出/校验 CLI | 本地导出行数、日期、主键、哈希全部一致 |
| C | 存量上传、云端导入与双写观察 | 云端连续 5 个交易日覆盖率不低于本地 |
| D | 分钟/竞价增量和服务迁云 | Mac 关闭采集后云端连续 5 个交易日无缺口 |
| E | Lab 按需路由和统一运行规格 | 切控件不执行隐藏页面，三策略使用同一 RunSpec |
| F | 统一任务中心与结果驾驶舱 | 所有长任务可关闭页面、恢复、取消、查看 ETA |
| G | 实验对比、晋级和模拟盘衔接 | 候选必须通过覆盖率、样本外和可成交性门禁 |
| H | Mac 研究主库退役 | 云端两份可恢复备份 + 10 个交易日稳定后才能删除本地主库 |

## 3. Task 1: 完成 Push 风暴根治

**Files:**
- Create: `src/rquant/notify/gate.py`
- Modify: `src/rquant/notify/api.py`
- Modify: `src/rquant/config.py`
- Modify: `src/rquant/cli.py`
- Modify: `scripts/alert-on-failure.sh`
- Modify: `scripts/monitor-watchdog.sh`
- Modify: `deploy/systemd/rquant-monitor.service`
- Modify: `deploy/com.roxor.rquant-monitor.plist`
- Test: `tests/unit/test_notification_gate.py`
- Test: `tests/unit/test_notify_api.py`
- Test: `tests/unit/test_cli.py`

**Step 1:** 写失败测试，复现同一错误被 CLI、OnFailure、watchdog 和多个重启重复发送。

**Step 2:** 运行 `pytest -q tests/unit/test_notification_gate.py tests/unit/test_notify_api.py`，确认因缺少持久 gate 失败。

**Step 3:** 使用独立 SQLite WAL 索引与 `flock` 文件门实现事故状态机：发送前只占 60 秒
pending 租约，成功后才进入 1800 秒冷却；全部通道失败时释放，两个存储均不可用时 fail closed。

**Step 4:** monitor/surge-watch 异常只由 systemd 告警；watchdog 和 OnFailure 使用相同
`dedup-key`，服务恢复后显式关闭事故，使之后的新故障能够再次通知。

**Step 5:** monitor 添加 `StartLimitIntervalSec=1800`、`StartLimitBurst=3`；Mac plist 设置 `NOTIFY_ENABLED=false`。

**Step 6:** systemd unit 走独立基础设施 PR 和 rollout；在腾讯云运行 `systemd-analyze verify`、
安装/回滚演练，再单独走代码 tag 的受控部署。

**Step 7:** 提交 `fix(notify): add persistent alert suppression and restart circuit breaker`。

## 4. Task 2: 建立研究数据湖契约

**Files:**
- Create: `src/rquant/research_lake.py`
- Create: `src/rquant/research_catalog.py`
- Create: `tests/unit/test_research_lake.py`
- Modify: `src/rquant/config.py`
- Modify: `src/rquant/data_contracts.py`
- Modify: `src/rquant/cli.py`
- Modify: `.env.example`

**Step 1:** 测试 `minute_bar` 和 `auction_bar` 的分区路径、schema、主键和 source 口径。

**Step 2:** 定义 `ResearchPartitionManifest` Pydantic 模型，至少包含 dataset、partition、行数、
最早/最晚时间、schema hash、content hash、source、created_at 和 code commit。

**Step 3:** 实现单分区临时写入、DuckDB 校验和 `os.replace` 原子发布；禁止直接追加现有 Parquet。

**Step 4:** 在 `research.duckdb` 建 `research_partition`、`research_ingest_run`、
`research_dataset_coverage`，只用短事务写元数据。

**Step 5:** 新增 `rquant research-export --dataset minute_bar|auction_bar --start-date ... --end-date ... --dry-run`。

**Step 6:** 验证同一分区重复导出幂等，内容变化必须生成新 manifest 并留下替换证据。

**Step 7:** 提交 `feat(research): add partitioned research data lake contracts`。

## 5. Task 3: 迁移存量研究数据

**Files:**
- Create: `scripts/migrate-research-to-cloud.sh`
- Create: `docs/deploy/research-cloud-bootstrap.md`
- Create: `tests/unit/test_research_migration_script.py`
- Modify: `src/rquant/cli.py`

**Step 1:** 盘后停止本地 monitor，执行 DuckDB checkpoint，并生成不可变本地恢复快照。

**Step 2:** 从本地只导出研究表，不覆盖云端生产表：`minute_bar`、`auction_bar`、价量分布、
分钟特征、回测记录、模拟盘和研究 manifest。

**Step 3:** 为每个交易日分区生成行数、时间范围、主键重复数和 SHA-256 manifest。

**Step 4:** 使用 `rsync --partial --checksum` 上传到云端 staging；上传中断可续传，不发布半文件。

**Step 5:** 云端逐分区复核 manifest 后原子移入 `data/lake`，刷新 `research.duckdb` 目录。

**Step 6:** 比较本地/云端总行数、交易日集合、每分区行数、随机 100 个键和聚合金额。

**Step 7:** 保留本地主库至少 10 个交易日；在此之前只允许标记“云端候选权威”。

**Step 8:** 提交 `feat(research): migrate historical minute and auction partitions to cloud`。

## 6. Task 4: 每日增量与服务迁云

> 2026-07-17 进度：应用层已实现不可变 monitor 盘前清单证据、日终 `rt_min_daily`/竞价补齐、
> 分钟 241 格与竞价 98% 覆盖门、带 journal 自动回滚的 lake/catalog 双数据集事务发布、
> 全局 publisher 锁与回滚 CAS、`research_ro` 刷新和可验证 observation/Parquet 哈希链的
> 10 日观察入口；晋级时全量复核 bootstrap catalog。生产开关默认为关闭；systemd 18:10 调度、
> 首次启用和连续 5/10 个交易日观察仍是独立基础设施/运营阶段门，Mac 采集尚未卸载。

**Files:**
- Create: `src/rquant/research_ingest.py`
- Create: `deploy/systemd/rquant-research-ingest.service`
- Create: `deploy/systemd/rquant-research-ingest.timer`
- Create: `tests/unit/test_research_ingest.py`
- Modify: `src/rquant/monitor.py`
- Modify: `src/rquant/surge_watch.py`
- Modify: `src/rquant/auction_backfill.py`

**Step 1:** 先写 PIT 测试：盘中只能落当时已返回的分钟；日终补齐不得改写原始 source 证据。

**Step 2:** monitor 将 pool/watchlist 分钟写入当日 staging，日终用 `rt_min_daily` 补齐并封分区。

**Step 3:** surge-watch 不保存全市场无限历史，只保存策略候选和入选前后窗口；若未来决定全市场
留存，必须先启用 COS 生命周期和容量预算。

**Step 4:** 09:26 后采集集合竞价，记录 source 与 available_at；09:30 分钟 fallback 单独分区。

**Step 5:** systemd timer 在 18:10（等待 17:00 daily 与最新副本）做补齐、校验、封分区和
research_ro 原子刷新；15:15 仅作为手工历史补跑的最早安全门。

**Step 6:** 连续 5 个交易日比较 Mac/云端资格股票日、分钟覆盖率和竞价覆盖率。

**Step 7:** 云端覆盖通过后卸载 `com.roxor.rquant-monitor`；Mac 只保留按需研究 worker。

**Step 8:** 提交 `feat(research): make cloud ingestion authoritative`。

## 7. Task 5: Lab 按需路由，解决卡顿

**Files:**
- Create: `src/rquant/dashboard/lab/pages/*.py`
- Create: `src/rquant/dashboard/lab/router.py`
- Create: `tests/unit/test_strategy_lab_router.py`
- Modify: `src/rquant/dashboard/strategy_lab.py`

**Step 1:** 测试选择 N 字页面时，不导入也不执行集合竞价、科创创业和历史页查询。

**Step 2:** 用侧边栏单选/分段导航替代 `st.tabs`；每次 rerun 只调用一个页面 renderer。

**Step 3:** 将当前 2,400 行单文件按运行、结果、历史、任务中心拆分；页面函数只接收 typed context。

**Step 4:** 所有参数放入 `st.form`，编辑控件不触发数据扫描，提交后才构建运行规格。

**Step 5:** 为昂贵查询加参数化缓存与明确 TTL，不缓存持写连接或 DataFrame 全历史。

**Step 6:** Playwright 验证桌面和手机宽度无重叠，切换导航不丢已保存结果。

**Step 7:** 提交 `refactor(lab): execute only the active strategy workspace`。

## 8. Task 6: 统一 StrategyRunSpec 和运行前体检

**Files:**
- Create: `src/rquant/strategy_spec.py`
- Create: `src/rquant/dashboard/lab/run_form.py`
- Create: `tests/unit/test_strategy_run_spec.py`
- Modify: `src/rquant/dashboard/strategy_lab_worker.py`
- Modify: `src/rquant/research_gate.py`

**Step 1:** 定义统一 Pydantic `StrategyRunSpec`：策略版本、股票范围、日期、B/S、持有期、费用、
复权口径、数据快照、样本外切分和随机种子。

**Step 2:** N 字、集合竞价、科创创业分别提供 adapter，但进入 worker 后只接收统一 spec。

**Step 3:** 提交前显示资格分母、分钟覆盖、竞价覆盖、缺失原因、预计扫描量和 ETA。

**Step 4:** 正式回测缺数据时禁用运行；探索模式允许执行但结果永久标记为探索性。

**Step 5:** 保存 canonical JSON 和 hash；同一 spec + snapshot 必须可复现同一交易集合。

**Step 6:** 提交 `feat(lab): unify strategy run specifications and preflight`。

## 9. Task 7: 统一后台任务中心

**Files:**
- Create: `src/rquant/lab_jobs.py`
- Create: `src/rquant/dashboard/lab/job_center.py`
- Create: `tests/unit/test_lab_jobs.py`
- Modify: `src/rquant/dashboard/strategy_lab_worker.py`

**Step 1:** 将 JSON 状态文件迁为独立 SQLite WAL，任务包含 queued/running/succeeded/failed/cancelled。

**Step 2:** 所有策略和消融都走同一队列；Streamlit 只提交任务和读状态，不执行回测线程。

**Step 3:** worker 每完成一个候选或日期分片更新 completed/total、EWMA ETA 和心跳。

**Step 4:** 支持取消、进程崩溃后的 stale lease 恢复、失败堆栈和一键按原 spec 重跑。

**Step 5:** 云端 worker 使用 CPUQuota/IOWeight，09:15-15:10 只允许轻任务。

**Step 6:** 提交 `feat(lab): add durable strategy job center`。

## 10. Task 8: 结果驾驶舱与实验对比

**Files:**
- Create: `src/rquant/strategy_result.py`
- Create: `src/rquant/dashboard/lab/result_overview.py`
- Create: `src/rquant/dashboard/lab/run_compare.py`
- Create: `tests/unit/test_strategy_result.py`
- Modify: `src/rquant/dashboard/strategy_lab_runs.py`

**Step 1:** 定义统一指标：资格数、触发数、成交数、覆盖率、样本内/外收益、胜率、盈亏比、
最大回撤、MAE/MFE、持有期、换手、费用、涨停不可买和 T+1 不可卖次数。

**Step 2:** 首屏只显示结论、可信度和主要风险，明细表按需展开，不把大表作为入口。

**Step 3:** 支持勾选 2-5 次运行，展示唯一变化参数、指标差值和相同交易日的配对差异。

**Step 4:** 消融对比自动生成“拿掉该因子后改善/恶化”的自然语言解释，但不替用户宣布最优。

**Step 5:** 历史记录支持策略、日期、状态、标签和代码版本检索，并保留 Markdown/JSON 导出。

**Step 6:** 提交 `feat(lab): add decision-focused results and run comparison`。

## 11. Task 9: 晋级模拟盘与 Mac 退役

**Files:**
- Create: `src/rquant/strategy_promotion.py`
- Create: `src/rquant/dashboard/lab/promotion.py`
- Create: `tests/unit/test_strategy_promotion.py`
- Modify: `src/rquant/paper.py`
- Modify: `docs/production-release.md`

**Step 1:** 候选必须引用不可变 spec、代码 commit、dataset snapshot 和样本外证据。

**Step 2:** 达到 roadmap 门槛后才能从 comparable 晋级 paper_candidate，参数晋级后冻结。

**Step 3:** 模拟盘记录每分钟决策因子、订单意图、T+1 冻结、实际可成交价和未成交原因。

**Step 4:** 连续 20 个前瞻交易日后比较历史回放与前瞻偏差，偏差超阈值自动降级。

**Step 5:** 云端存在原始分区、研究目录快照、异机备份三份证据，且连续 10 个交易日稳定后，
生成 Mac 删除 dry-run 清单；用户确认后才删除本地 `rquant.duckdb`。

**Step 6:** 提交 `feat(research): promote validated runs and retire local authority`。

## 12. 验收命令

```bash
# 通知与 CLI
pytest -q tests/unit/test_notification_gate.py tests/unit/test_notify_api.py \
  tests/unit/test_notify_client.py tests/unit/test_cli.py

# 云端 unit 语法
systemd-analyze verify /etc/systemd/system/rquant-monitor.service

# 数据迁移验收
rquant research-export --dataset minute_bar --start-date 2025-01-01 \
  --end-date 2026-07-16 --dry-run
rquant research-coverage --compare /path/to/local-manifest.json \
  /path/to/cloud-manifest.json

# Lab
pytest -q tests/unit/test_strategy_lab_*.py tests/unit/test_strategy_run_spec.py \
  tests/unit/test_lab_jobs.py tests/unit/test_strategy_result.py
```

## 13. 付费资源决策

- 当前存量和 pool/候选增量不需要立刻购买新服务器，现有 74GB 可用空间足够完成迁移观察。
- 在以下任一条件出现前启用腾讯 COS：云端磁盘使用率达到 60%、预计 90 天内达到 70%、
  或决定保存全市场逐分钟历史。
- 自动优化经常超过 30 分钟且影响盘中服务时，优先增加独立 4-8 vCPU 研究实例，不直接升级
  生产机后让两个工作负载继续抢资源。
- 不为解决网页反爬购买代理作为主方案；优先使用已经付费并验证可在云端运行的 Tushare 接口。
