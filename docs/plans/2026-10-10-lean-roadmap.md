# rQuant 精简路线图（2026-10-10）

依据：Claude Code 计划 v2（`frontend-plan-v2.md`，M0–M16）。M11 实盘下单不做。
接手 PR #324（`mvp/react-readonly`）的做法不变：**不要** admission / authority / projection / evidence / ledger 这些层，测试与代码量相称，每个模块一个 PR，层层叠在前一个 PR 上（stacked），改服务器之前先问用户。
Codex 的工作已归档在 tag `archive/codex-20261010`（box 上 `/workspace/cdx`）。下文说“复用”，指按目录或文件拿过来再删减，不 cherry-pick 提交。

## #324 已完成
M0 外壳、只读 API、OpenAPI 类型、轻量 CI；M1 总览、系统健康、市场全景；M4 选股器加排名（只读）、池子保存；M7 回测列表和逐笔明细；M10 模拟盘只读页；M12 告警时间线、确认（`alert_ack` 投影）；C4.8 加入盯盘（`manual_watchlist` 投影，盯盘程序读取）；部署 runbook；一期网页已上线（读回放数据）。

## 模块顺序

| # | 模块 / PR | 一句话范围 | 从 Codex 复用 | 不做 | 依赖 | 规模 |
|---|---|---|---|---|---|---|
| 1 | M7 绩效库 + 回测结果页 | `rquant.perf` 纯函数（净值、回撤、夏普、索提诺、卡玛、月度、round-trip）；回测详情 API 由逐笔交易现算，页面加 KPI、净值+回撤图、月度热力 | `src/rquant/perf/{core,trades}.py`、`tests/unit/test_perf_metrics.py` | 基准与超额（等模块 3）、HTML 导出、quantstats 对照 JSON | #324 | S（约 600 行） |
| 2 | M9 仓位规则 + 回撤控制 | `rquant.portfolio`：等权、按分加权、单票和行业上限、最多持股数、现金比例；回撤降仓 | `src/rquant/portfolio/{weights,drawdown}.py` | 优化器、Brinson（放到 6） | 无 | S |
| 3 | M6 基准指数采集 | Tushare `index_daily`（沪深300、中证1000、创业板指）进主库，回测页显示超额 | `factor/index_collect.py` 只取采集部分 | 指数成分、行业分类 | 1 | S |
| 4 | M6 通用日线组合回测 | `rquant.backtest.portfolio`：调仓、A 股规则（T+1、涨跌停、停牌、整手）、成本；CLI 任务产出 parquet+json，web 列表读取 | `portfolio_backtest_models.py` 的模型部分，`backtest/` 撮合 | 接 Job Center/Lab、artifact 索引表（先用文件目录）、统一引擎 | 2、3 | M |
| 5 | M8 过拟合指标 + 实验对比 | `rquant.overfit`（PSR、DSR、PBO、MinTRL）；实验列表和两条对比页 | `overfit.py`、`overfit_pbo.py` | 实验登记写入门、解封次数控制 | 1、4 | S–M |
| 6 | M9 行业暴露与简单归因 | 回测产物里的行业权重和 Brinson（BF 形式） | `portfolio/exposure.py` | 模拟盘按权重（serving 窗口） | 4 | S |
| 7 | M1 数据中心：目录与审计 | 数据字典（契约+中文说明）和覆盖率、缺口审计 CLI；数据中心页只读 | `data_catalog/`、`data_audit_{coverage,quality,datasets}.py` 的计算部分 | report job/projection/evidence 层、回补执行 | 无 | M |
| 8 | M3 因子库 + 单因子检验 | 受限表达式（cs_/ts_ 算子）、IC/分组/衰减检验 CLI，因子页读结果文件 | `factor/time_series.py`、`registry.py` 的计算部分 | 因子 admission/ledger/stream、因子跟踪定时 | 7 | L |
| 9 | M3 因子跟踪 | 每日增量 IC，跟踪面板 | `factor/tracking.py` 计算部分 | runner/serving 层 | 8 | S |
| 10 | M5 策略模板与版本 | 模板（入场池子、退出规则、仓位）编译成 spec，存 JSON 版本，回测可按版本复现 | `experiment_platform_templates.py` 的模型 | 晋级门、审批流 | 4 | M |
| 11 | M12 告警规则配置 | 规则（范围、条件、频率、去重），规则增删改走 page-control，盯盘程序读取 | `alert_rule_contracts.py` | 新运行时告警 role、通道统计 | #324 | M |
| 12 | M13 任务总览 + 日志（只读） | systemd 定时任务状态和最近日志（白名单、脱敏），由只读 root 小服务提供 | `ops/` 只读部分 | 立即运行（需另行授权）、研究任务控制 | 服务器授权 | M |
| 13 | M4 盘中条件 + 通达信公式导入 | 用积木筛最新快照；tdx 子集解析成 MyTT | `screen/` 相关解析 | 公式市场、formula jobs | 选股器 | M |
| 14 | M10 模拟盘区间对照 | 回测日收益自助抽样，得到 5–95% 区间，叠在模拟净值上 | 无 | 对账任务、暂停账户 | 4 | S |
| 15 | M14 AI：回测解读 | DeepSeek 按模板解读回测，数字逐个核对 | `llm/` 客户端 | 公告摘要（等积分）、Agent | 1、5 | S |
| 16 | M2 只读查询页 | 单条 SELECT、超时和行数上限，查只读副本 | `research_query/` 校验部分 | Jupyter 沙箱（需授权+HTTPS） | 服务器授权 | M |
| 17 | M1 财务基本面（PIT） | fina_indicator 等采集，按公告日可见；选股积木 | — | 公告/新闻 | Tushare 积分 | L |
| 18 | M15 操作记录 + 简单角色 | page-control 回执列表页；admin/viewer 两级 | — | 细粒度 RBAC | 11 | S |
| 19 | M16 分层健康页 | 健康页按六层组织 | — | 新心跳字段（serving 窗口） | 7、12 | S |
| — | 统一引擎（C6.3） | 暂缓：难度最大，等 4、10 稳定后再评估 | — | — | 4、10 | XL |

规模：S 不到 800 行，M 800–2000 行，L 超过 2000 行（含测试）。

## 共同规则
- 计算库都是纯函数，单测对照手算的小样例；不引入 quantstats、alphalens 依赖。
- 研究结果先落文件（`data/research/<kind>/<id>/`），web 只读；以后要上 serving 时，统一在一个“产物索引”PR 里做。
- 任何改服务器的步骤（新 unit、nginx、serving schema 窗口）都写进 runbook，等用户批准后再执行。
