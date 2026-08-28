---
title: rQuant - 个人版 A 股量化选股与监控平台
created_at: 2026-04-15
updated_at: 2026-07-18
status: active
owner: roxor
tags: [quant, a-shares, personal-tool, python, macOS]
---

# rQuant

rQuant 是个人自用的 A 股条件筛选、分钟监控与告警平台。它包含历史回放、策略研究和自动
模拟盘，但**不做实盘下单**，也不把回测收益当作未来收益承诺。

## 当前能力

- 收盘后更新日线、指标、市场状态，生成 Pool1/Pool2 等候选池。
- 接入 Tushare 历史 1 分钟、实时分钟、实时分钟日累计和集合竞价。
- 对候选池运行盘中分钟监控，通过 PushDeer/PushPlus 告警。
- 云端是盘中 monitor 与系统异常的唯一告警权威；同一事故跨进程/重启默认只通知一次，
  服务恢复后才关闭事故。本地 monitor 只采集；尚未迁云的晨间脉搏/午间战报仍是独立业务报告。
- Strategy Lab 支持 N 字、集合竞价和科创/创业放量的 replay、消融与研究记录。
- 20 个首批策略数据集已有 Point-in-Time 契约；历史名称/ST、复权价格、竞价/分钟/盘后数据
  可见性均按决策时点 fail closed，研究快照、覆盖率、质量问题和权威交易日历可追溯。
- 正式研究由持久化数据审计与覆盖率门禁保护；Tushare 停复牌事实按交易日保存，不再用零成交量
  猜测停牌。
- `research-export` 可从只读副本把分钟/竞价数据发布为校验过的交易日 Parquet 分区，并在独立
  `research.duckdb` 保存 manifest、覆盖度和替换审计；支持零落盘 dry-run。
- `research-migration` 与分阶段迁移脚本可从同一不可变恢复快照打包、校验、断点上传并在云端
  原子发布研究候选；生产 `rquant.duckdb` 不进入迁移写路径。
- `research-ingest` 可在日终补齐不可变 monitor 清单分钟、拉取集合竞价，并通过可自动回滚的
  发布 journal 一致切换 Parquet manifest、研究主/只读目录和观察证据；日终 runner 会先确认
  daily 成功、主动刷新副本并检查当日日线完整性，漏日可按证据链顺序用 `stk_mins` 恢复。
- `research-repair-auction` 可对位于证据链中间的集合竞价历史缺口做真实取数预演，再凭内容
  绑定的 plan id 批次原子发布；旧内容寻址版本继续可读，修复后 10 日稳定观察重新累计。
- `research-repair-minute` 可按已完成的策略回补 manifest，把生产只读副本中已具备、研究湖
  尚缺的完整 241 分钟会话受控合入历史分区；完整性检查分批返回标量，两遍式修复每次只
  保留一个交易日的分钟帧。预演不请求行情接口，正式执行须复用同一个内容绑定 plan id，
  并在任一来源漂移或发布边界失败时整批回滚。
- `formal-smoke-replay` 以固定 v1 参数在不可变 execution binding 上运行三策略正式冒烟
  回放；它要求精确审计、snapshot、binding 和真实干净 Git HEAD，不允许用环境变量伪造
  提交身份，也不允许回退到探索模式或滚动数据库。
- 云端承担生产调度和研究候选存储；Mac 主库在 10 个交易日观察完成前继续保留为恢复依据。

## 明确边界

- 不接券商交易接口，不自动下单。
- 不做高频、Tick 级微观结构或 Level2。
- 分钟 OHLCV 近似不能冒充真实内外盘或订单失衡。
- 自动优化只在离线研究区运行，不能自动发布到 live。

## 当前研究状态

从 2026-07-13 起，研究结果分为四级：

| 状态 | 含义 |
|---|---|
| `exploratory` | 探索性；数据、成交或样本仍不完整 |
| `comparable` | 可在统一数据和执行口径下横向比较 |
| `paper_candidate` | 严格样本外通过，参数冻结，可进入模拟盘 |
| `monitor_approved` | 前瞻模拟达到门槛，可进入正式监控提醒 |

当前 N 字、科创/创业放量、集合竞价独立策略均为 `exploratory`。主要原因是分钟覆盖、
成交可行性、live/replay 语义和严格样本外验证尚未全部闭环。

2026-07-18 Stage 1 数据审计保持 `stage1-v3` P0=0，不可变 snapshot/binding 执行链和
受控历史分钟研究湖修复已上线。`v0.25.0` 将分钟修复改为有界内存两遍式，并为三策略增加
只能通过 formal gate 的固定冒烟回放。`v0.25.1` 统一空时段全天停牌证据、整段原子刷新
和可恢复三策略生产验收。生产实测证明串行分钟回补 ETA 低估 17 到 27 倍；`v0.25.2`
改为并发拉取、DuckDB 单写，以历史任务遥测生成保守 ETA，并允许在硬保护截止前分段续跑。
新正式证据完成前，三策略仍保持 `exploratory`。

**可用资格截止日是移动的，单份 manifest 是冻结的。** 每次执行 `backfill-plan` 且省略
`--end-date` 时，系统按权威交易日历、当前已完整收盘交易日、策略入场偏移和完整 B/S 窗口
重新计算最新资格日；时间前进后，新计划的截止日会前进。计划一旦持久化，其 `as_of_time`、
资格集合、窗口、输入哈希和代码提交永久不变。要覆盖新日期必须创建新 manifest，预演、
修复或快照都不能悄悄扩大旧 manifest。

`v0.23.2` 已修正 N 字 121 交易日资格面板对
“完整停牌快照、只有停牌无复牌、且无日线”这一确定性非交易日的完整性语义。代码能力完成
不等于策略结论已可信；N 字、科创/创业放量和集合竞价仍需分别补齐真实资格 manifest，
达到 baseline 95%、eligibility/B/S 99% 并完成生产固定回放后，才能从 `exploratory`
晋级。

- [研究可信度基线](docs/analysis/2026-07-13-research-trust-baseline.md)
- [2026-07-22 可信研究平台与策略闭环实施计划](docs/plans/2026-07-22-rquant-trustworthy-research-platform-implementation.md)
- [阶段 1 真实数据验收](docs/analysis/2026-07-15-stage1-data-contract-acceptance.md)
- [v0.17.1 Stage 1 状态修复与首次部署](docs/deploy/2026-07-15-v0.17.1-stage1-bootstrap.md)
- [可信策略研究与盘中监控路线图](docs/plans/2026-07-13-rquant-trustworthy-strategy-roadmap.md)
- [不可变执行快照设计](docs/plans/2026-07-17-stage1-execution-snapshot-design.md)
- [研究数据云化、告警治理与 Strategy Lab 重构计划](docs/plans/2026-07-16-research-cloud-alert-lab-implementation.md)
- [Strategy Lab 与持久 Job Center 自动优化说明](docs/strategy-lab-auto-optimization-guide.md)

## 运行架构

```text
腾讯云 82.156.0.68                    本地 Mac
-------------------------------      --------------------------------
daily / monitor / notifications      历史分钟 / 竞价 / 策略研究
生产 DuckDB                           研究 DuckDB
systemd timers                        Strategy Lab / panorama / launchd
        |                                      ^
        +---- cloud snapshot -> backup --------+
                             research-sync 合并生产表
```

DuckDB 是单文件锁。盘中唯一写入者是 monitor；Dashboard、Lab 和临时查询必须读
`rquant_ro.duckdb` 副本。云端快照下载到 `cloud_backup.duckdb`，禁止整文件覆盖本地主库。
研究湖导出和首次迁移已落地，存量数据按恢复快照、迁移包、staging、候选发布四阶段进入
独立 `research.duckdb` 与交易日分区 Parquet。每日增量代码使用隔离代际和持久 journal 完成
分钟、竞价双数据集可恢复发布，`research-authority-status` 同时验证 catalog、manifest、文件
哈希与连续 observation 证据链。每日调度和 10 个交易日观察
期全部验收前，本地 `rquant.duckdb` 仍是分钟/竞价研究数据权威，不得删除。完整步骤见
[研究数据首次迁云操作手册](docs/deploy/research-cloud-bootstrap.md)。可先用以下命令只读估算：

隔离运行时的当前入口与验收边界见[工作负载解耦设计](docs/architecture/2026-07-22-workload-isolation-design.md)：
代码主体及本地专项/全量归因已完成，但 Linux CI、云端 systemd 和真实交易日 shadow 仍待验收，
旧链路继续保留对账；发布门见[受控自动发布](docs/production-release.md)。

```bash
rquant research-export --dataset minute_bar \
  --start-date 2025-03-28 --end-date 2026-07-16 --dry-run

# 日终只读预演与候选状态核验
rquant research-ingest-readiness --date 2026-07-17
rquant research-ingest --date 2026-07-17 --dry-run
rquant research-ingest --date 2026-07-16 --recover
rquant research-authority-status

# 历史竞价修复先真实取数预演，再原样复用日期集和输出的 plan_id 执行
rquant research-repair-auction \
  --date 2026-04-20 \
  --date 2026-07-07
rquant research-repair-auction \
  --date 2026-04-20 \
  --date 2026-07-07 \
  --apply \
  --plan-id <预演输出的plan_id>

# 已完成的策略分钟回补 manifest：先只读核对研究湖缺口，再复用同一 plan_id 原子修复
rquant research-repair-minute --manifest-id <已完成的manifest_id>
rquant research-repair-minute \
  --manifest-id <同一个manifest_id> \
  --apply \
  --plan-id <预演输出的plan_id>

# 长分钟 manifest：网络并发、DuckDB 单写；09:05 停写、09:10 硬退出
rquant backfill-run \
  --manifest-id <manifest_id> \
  --workers 8 \
  --max-runtime-minutes 1050
```

## 技术栈

| 层 | 选型 |
|---|---|
| Python | 3.11+（云端 3.11.6，本地 3.12），uv |
| 数据 | Tushare Pro + AKShare/Ashare 兜底 |
| 存储 | DuckDB + Parquet + JSON/JSONL |
| 指标/计算 | pandas、ta、MyTT |
| 调度 | 云端 systemd + 本地 launchd/APScheduler |
| UI | Streamlit |
| 通知 | PushDeer + PushPlus |
| 测试 | pytest + ruff + GitHub Actions |

## 本地开发

```bash
cd /Users/roxor/brain/30-projects/rQuant
uv sync --frozen
cp .env.example .env
# 填写 Tushare token 和本地绝对路径后：
uv run pytest -q
bash scripts/check-core-quality.sh
```

`.env` 包含 token、登录信息和通知凭据，禁止提交。测试默认禁用真实通知。
全仓历史 lint 债务会分阶段清理；当前 CI 的强制范围由 `check-core-quality.sh` 统一维护。

## 常用入口

```bash
# CLI 帮助
.venv/bin/rquant --help

# 日度链路预检
.venv/bin/rquant preflight
.venv/bin/rquant preflight --profile research

# 运行并保存阶段 1 数据审计（P0 时返回非零）
.venv/bin/rquant data-audit --start-date 2026-04-01 --as-of 2026-07-14

# 先估算历史名称/ST 回补规模，再在盘外安全窗口执行
.venv/bin/rquant security-status-backfill \
  --start-date 2026-04-01 --end-date 2026-07-14 --dry-run
.venv/bin/rquant security-status-backfill \
  --start-date 2026-04-01 --end-date 2026-07-14

# 回补权威停复牌逐日快照
.venv/bin/rquant suspension-backfill \
  --start-date 2026-04-01 --end-date 2026-07-14

# 复权因子历史不完整时，先仅回补 adj_factor（不会改日线/状态表）
.venv/bin/rquant data-backfill --dataset adj_factor \
  --start-date 2024-09-02 --end-date 2026-07-16 --dry-run
.venv/bin/rquant data-backfill --dataset adj_factor \
  --start-date 2024-09-02 --end-date 2026-07-16

# 修复缺失日线时，市场事实先短事务写入；状态尾段最后原子重建
# 受影响的日指标会明确失效，随后必须运行 daily-indicator-backfill
.venv/bin/rquant market-daily-backfill \
  --start-date 2026-04-20 --end-date 2026-04-20

# 再预演，并在盘外窗口从本地日线与复权因子重建日指标
.venv/bin/rquant daily-indicator-backfill \
  --start-date 2026-03-31 --end-date 2026-07-16
.venv/bin/rquant daily-indicator-backfill \
  --start-date 2026-03-31 --end-date 2026-07-16 --apply

# 健康看板
.venv/bin/streamlit run src/rquant/dashboard/app.py --server.port 8501

# Strategy Lab（提交到持久 Job Center；关闭页面或切换页签不影响后台任务）
.venv/bin/streamlit run src/rquant/dashboard/strategy_lab.py --server.port 8504

# 盘中全景
.venv/bin/streamlit run src/rquant/dashboard/market_panorama.py --server.port 8506

# 生成可恢复的策略分钟回补计划（只读副本；截止日自动移动到完整 B/S 窗口可观测上限）
.venv/bin/rquant backfill-plan \
  --strategy growth_board_surge \
  --start-date 2025-01-01

# 需要固定历史区间时可显式指定更早日期；超过可观测上限会失败关闭
.venv/bin/rquant backfill-plan \
  --strategy growth_board_surge \
  --start-date 2025-01-01 \
  --end-date 2026-06-30

# 盘外执行（领取任务前会重验 manifest 的可观测窗口）、查询进度并固化研究元数据
.venv/bin/rquant backfill-run --manifest-id <64位ID>
.venv/bin/rquant backfill-status --manifest-id <64位ID> --json

# 策略退役时先预演；核对任务计数和原因后，复用输出的精确 plan_id 才能写入终态
.venv/bin/rquant backfill-abandon \
  --manifest-id <64位ID> \
  --reason "策略研究线已退役"
.venv/bin/rquant backfill-abandon \
  --manifest-id <64位ID> \
  --reason "策略研究线已退役" \
  --plan-id <预演输出的plan_id> \
  --apply

# 单个策略的 Stage 1 只读验收预演；正式链路见 docs/deploy/2026-07-22-stage1-strategy-closeout.md
.venv/bin/rquant stage1-acceptance \
  --strategy n_shape \
  --manifest-id <64位ID> \
  --start-date 2026-04-01 \
  --end-date <manifest的effective_end_date> \
  --expected-code-commit <40位Git SHA>

.venv/bin/rquant dataset-snapshot \
  --strategy growth_board_surge \
  --as-of 2026-06-30T15:00:00+08:00 \
  --manifest-id <64位ID> \
  --dry-run

# 核对预演后在不会跨入交易保护窗口的时段生成元数据快照与不可变执行绑定
.venv/bin/rquant dataset-snapshot \
  --strategy growth_board_surge \
  --as-of 2026-06-30T15:00:00+08:00 \
  --manifest-id <64位ID> \
  --apply

# 只通过正式门运行固定规格；三项证据均来自同一次已完成的 audit/snapshot/binding
.venv/bin/rquant formal-smoke-replay \
  --strategy growth_board_surge \
  --start-date 2025-01-01 \
  --end-date 2026-06-30 \
  --audit-run-id <64位ID> \
  --snapshot-id <64位ID> \
  --binding-hash <64位SHA256>
```

本地页面默认分别访问 `http://127.0.0.1:8501`、`http://127.0.0.1:8504` 和
`http://127.0.0.1:8506`。手机跨网络访问使用云端部署入口，不直接暴露 Mac 端口。

## 数据与回测原则

1. 先用信号时点可见的日级条件确定资格全集，再计算分钟覆盖率。
2. 第 `t` 分钟收盘确认信号，最早按 `t+1` 分钟尝试成交。
3. 日级盘后资金流、收盘涨停家数不能用于同日盘中 B 信号。
4. B 日不能 S；止损只能记录风险，最早下一交易日执行。
5. 原始价、复权因子、涨跌停价和收益口径必须显式区分。
6. 测试区间只评分，不参与选参数；参数晋级还需前瞻模拟。
7. 研究数据必须声明最早可见时刻；集合竞价 Tushare 行最早按 09:26、09:30 分钟 fallback
   最早按 09:31 使用，缺失来源信息时失败关闭。
8. 交易日只认持久化权威日历；已知周末/节假日是休市，缺行是数据问题，不能用日线反推。
9. 历史回放的 `as-of` 是事件时间截止，不假装分钟回补文件在历史当时已存在；正式绑定另存
   分区发布时间、catalog 观察时间和 binding 生成时间，明确本次研究所用的数据版本。
10. 资格完整性分母来自上市股票、历史状态和日线键的并集；正式回放只读取 binding 内精确
    `strategy_eligibility` 候选，不能用当前 `screen_result` 重算或补充候选。
11. 集合竞价资格解析直接读取 manifest 固定的 `auction_bar` 研究湖版本；catalog 换头不能
    改写旧候选或正式回放输入。写入型 snapshot 有保守启动门禁和运行中硬 deadline。

## 目录

```text
src/rquant/           业务代码
tests/                单元与集成测试
docs/plans/           设计和实施计划
docs/analysis/        不可变研究基线与归因报告
docs/deploy/          专项部署清单
deploy/systemd/       云端 unit
deploy/launchd/       本地定时任务
scripts/              数据同步、部署和研究脚本
data/                 本地数据与运行状态，不进 Git
```

## 开发顺序

当前执行 [2026-07-13 总路线图](docs/plans/2026-07-13-rquant-trustworthy-strategy-roadmap.md)：

1. 冻结研究基线和工程护栏。
2. 建数据契约、PIT 状态、回补清单和不可变执行绑定（代码能力已完成；当前逐策略生成真实
   manifest、补齐覆盖并做生产固定回放验收）。
3. 统一无未来函数分钟特征和 StrategySpec。
4. 完善可成交性、费用和 10 万本金账户模拟。
5. 修正优化器后重评现有策略。
6. 参数冻结，运行至少 20、优选 40 个交易日的前瞻模拟盘。
7. 再逐个研究首板弱转强、龙虎榜、资金流和题材轮动。

一次只推进一个阶段，未通过阶段门不进入下一阶段。

## 部署纪律

- 日常代码发布由 Codex 代管，只部署已合并且 CI 全绿的 annotated tag 或指定完整 commit，
  不盲目跟随 main HEAD。
- 云端调用受控部署器，具备交易时段保护、最小 sudo 白名单、双 preflight、审计和自动回滚。
- systemd/nginx、sudoers 安装和生产数据写入仍是单独授权的高风险变更。
- systemd unit 修改必须先在云端通过 `systemd-analyze`/unit verify。
- 每次实际部署在 [DEPLOY.md](DEPLOY.md) 追加版本、验证和回滚命令。

操作说明见 [受控自动发布](docs/production-release.md)，完整项目约束见 [AGENTS.md](AGENTS.md)，
数据源现状见
[docs/data-sources-matrix.md](docs/data-sources-matrix.md)。
