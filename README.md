---
title: rQuant - 个人版 A 股量化选股与监控平台
created_at: 2026-04-15
updated_at: 2026-07-15
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
- Strategy Lab 支持 N 字、集合竞价和科创/创业放量的 replay、消融与研究记录。
- 20 个首批策略数据集已有 Point-in-Time 契约；历史名称/ST、复权价格、竞价/分钟/盘后数据
  可见性均按决策时点 fail closed，研究快照、覆盖率、质量问题和权威交易日历可追溯。
- 正式研究由持久化数据审计与覆盖率门禁保护；Tushare 停复牌事实按交易日保存，不再用零成交量
  猜测停牌。
- DuckDB 保存生产和研究数据，Parquet/JSONL 保存共享快照与任务结果。
- 云端承担生产调度，本地 Mac 承担研究、分钟数据和热备。

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

2026-07-15 的阶段 1 真实数据审计发现 2 个 P0：379,658 个日线资格键缺历史名称/ST，涨停池
含 400 行休市日数据。PR-D 的代码能力已经完成，但数据准入尚未通过；正式回测继续阻断，且
尚未启动大规模分钟下载。现有数据快照只冻结覆盖元数据，正式模式还会要求不可变计算快照，
不会让底层数据已变化的结果冒充可复现研究。

- [研究可信度基线](docs/analysis/2026-07-13-research-trust-baseline.md)
- [阶段 1 真实数据验收](docs/analysis/2026-07-15-stage1-data-contract-acceptance.md)
- [v0.17.1 Stage 1 状态修复与首次部署](docs/deploy/2026-07-15-v0.17.1-stage1-bootstrap.md)
- [可信策略研究与盘中监控路线图](docs/plans/2026-07-13-rquant-trustworthy-strategy-roadmap.md)
- [Strategy Lab 自动优化说明](docs/strategy-lab-auto-optimization-guide.md)

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

# 健康看板
.venv/bin/streamlit run src/rquant/dashboard/app.py --server.port 8501

# Strategy Lab
.venv/bin/streamlit run src/rquant/dashboard/strategy_lab.py --server.port 8504

# 盘中全景
.venv/bin/streamlit run src/rquant/dashboard/market_panorama.py --server.port 8506

# 生成可恢复的策略分钟回补计划（只读副本）
.venv/bin/rquant backfill-plan \
  --strategy growth_board_surge \
  --start-date 2025-01-01 \
  --end-date 2026-06-30

# 盘外执行、查询进度并在覆盖率达标后固化研究元数据
.venv/bin/rquant backfill-run --manifest-id <64位ID>
.venv/bin/rquant backfill-status --manifest-id <64位ID> --json
.venv/bin/rquant dataset-snapshot \
  --strategy growth_board_surge \
  --as-of 2026-06-30T15:00:00+08:00 \
  --manifest-id <64位ID>
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
2. 建数据契约、PIT 状态与回补清单（迁移内核、历史状态、质量审计、PIT 复权和可见性门禁
   已完成；真实数据审计已运行，当前先修复 2 个 P0，再验收三类策略覆盖率）。
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
