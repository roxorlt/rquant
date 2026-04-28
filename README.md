---
title: rQuant — 个人版 A 股量化选股与监控平台
created_at: 2026-04-15
updated_at: 2026-04-15
status: planning
owner: roxor
tags: [quant, a-shares, personal-tool, python, macOS]
---

# rQuant

个人自用的 A 股量化选股与盯盘平台。**不做实盘下单**，只做「条件筛选 + 实时监控 + 告警通知」。

## 定位

- **非交易时段**：用日级数据跑筛选规则，生成次日备选池
- **交易时段**：对备选池做分钟/准实时跟踪，触发条件时推送告警
- **核心能力**：可配置的筛选条件（GUI 或 DSL）+ 可视化面板 + 多渠道通知
- **明确不做**：实盘下单、高频策略、Tick 级微观结构分析、Level2 相关能力

## 为什么自己做

- 现成的 [myhhub/stock](https://github.com/myhhub/stock)、[qstock](https://github.com/tkfy920/qstock)、[QuantDinger](https://github.com/brokermr810/QuantDinger) 已经很接近这个场景
- 但个人量化工具最终都会向"顺手的工具链"演进，fork 一个最近的版本再按自己习惯改，比从零写更快
- 本项目的重心不是造轮子，而是**搭一个能持续演进的骨架**，承接后续选股逻辑的迭代

## 顶层架构（六层 + 横切）

```
┌───────────────────────────────────────────────────────────────┐
│  6. UI / 展示层 (Presentation)                                │
│     Streamlit Dashboard · CLI · IM 推送                       │
├───────────────────────────────────────────────────────────────┤
│  5. 应用 / 编排层 (Application / Orchestration)               │
│     定时调度 · 事件监听 · 告警分发 · 日志审计                  │
│     APScheduler / Prefect                                     │
├───────────────────────────────────────────────────────────────┤
│  4. 策略 / 规则引擎层 (Strategy / Rule Engine)                │
│     选股条件 DSL · 触发器 · 信号生成                           │
│     例：MA5 上穿 MA20 AND 换手率>3%                           │
├───────────────────────────────────────────────────────────────┤
│  3. 特征 / 指标层 (Feature / Indicator Store)                 │
│     技术指标 · 基本面衍生 · 自定义因子                         │
│     pandas-ta / TA-Lib / stockstats                           │
├───────────────────────────────────────────────────────────────┤
│  2. 存储层 (Storage / Data Warehouse)                         │
│     DuckDB + Parquet（冷）· SQLite（热状态）· 可选 Redis       │
├───────────────────────────────────────────────────────────────┤
│  1. 数据接入层 (Data Source Adapter)                          │
│     Tushare · AKShare · Ashare · mootdx                       │
└───────────────────────────────────────────────────────────────┘
          ↕ 横切关注点
          · 配置（Pydantic Settings）  · 日志（loguru）
          · 状态持久化（SQLite）      · 健康检查 · pytest
```

各层职责详见 [docs/architecture.md](docs/architecture.md)（待写，MVP 阶段从本 README 派生）。

## 技术栈选型（Mac 友好）

| 层 | 选型 | 理由 |
|---|---|---|
| 语言 | Python 3.11+ | 量化生态最成熟 |
| 包管理 | uv 或 poetry | 现代工具链 |
| 数据接入 | Tushare Pro（5000 积分 ¥500/年）+ AKShare + Ashare | 日级 + 实时覆盖够用 |
| 历史分钟补刀 | mootdx（pytdx 活跃 fork） | Mac 可用，通达信协议 |
| 存储 | DuckDB（主）+ Parquet（冷归档）+ SQLite（状态） | Mac 原生、单文件、SQL 友好 |
| 指标计算 | pandas-ta | 纯 Python，Mac 无坑（TA-Lib 装编译麻烦） |
| 调度 | APScheduler | 轻量够用，不上 Celery |
| 配置 | Pydantic Settings + `.env` | 类型安全 |
| 日志 | loguru | 零配置彩色日志 |
| UI | Streamlit | Python 一把梭，一周出原型 |
| 通知 | PushDeer（参考 xueqiuFollow） | 通道稳定，云端零迁移；cc2im 受限于微信 token |
| 测试 | pytest + 固定 fixture 数据 | 标准 |

## MVP 路径（8 周）

```
Week 1   [数据接入]   Tushare 拉日线 + AKShare 兜底 → DuckDB 存储能查
Week 2   [指标计算]   pandas-ta 算 MA/MACD/RSI/KDJ，落 DuckDB 缓存
Week 3a  [派生字段]   涨跌停/首板/一字板/连板/板块/ST/实体上下沿 → daily_state 表
Week 3b  [筛选规则]   原子条件"积木"函数库 + screen() 入口，支持多条件 AND + 跨日引用
                     命名对齐通达信/MyTT 风格（为 Week 8 铺路）
Week 4   [调度]       APScheduler 每日 17:00 自动拉数+跑筛选，结果落 screen_result 表
Week 5   [实时监控]   Ashare 盘中轮询备选池，触发条件打印告警
Week 6   [通知打通]   告警推送 PushDeer（5 类场景：档位/退出/汇总/异常/心跳）
Week 7   [UI + NL]    Streamlit 面板 + 自然语言输入筛选条件（LLM → 积木调用）
Week 8   [通达信代码] 支持粘贴通达信选股公式，解析器映射到 MyTT/积木执行
```

**原则**：每周"能跑"再进下一步，不要并行推进。

## 参考开源项目

- [myhhub/stock](https://github.com/myhhub/stock) — 最接近本项目场景
- [qstock](https://github.com/tkfy920/qstock) — 选股模块齐全
- [daily_stock_analysis](https://github.com/ZhuLinsen/daily_stock_analysis) — LLM 驱动的推送仪表盘
- [QuantDinger](https://github.com/brokermr810/QuantDinger) — 本地化 AI 量化平台
- [aiagents-stock](https://github.com/oficcejo/aiagents-stock) — 多 Agent 盯盘
- [easyquotation](https://github.com/shidenggui/easyquotation) — 实时行情组件

完整项目调研见 [docs/references.md](docs/references.md)。

## 数据源选型

简要：
- **日级数据**（T+1）：Tushare Pro ¥500/年 5000 积分 + AKShare 兜底
- **实时行情**：Ashare（新浪/腾讯双核爬取）
- **历史分钟补刀**：mootdx
- **不做**：Level2 / Tick / 实盘下单

完整的数据源竞品矩阵见 [docs/data-sources-matrix.md](docs/data-sources-matrix.md)。

## 目录约定

```
rQuant/
├── README.md                  # 本文件
├── CLAUDE.md                  # 给 Claude 的项目指令
├── docs/
│   ├── data-sources-matrix.md # 19 个数据源竞品矩阵
│   └── references.md          # 参考开源项目详情
├── src/rquant/                # 源码（MVP 阶段再建）
├── tests/                     # 单元测试
├── data/                      # 本地数据（.gitignore）
│   ├── warehouse/             # DuckDB / Parquet
│   └── state/                 # SQLite 状态
├── configs/                   # 配置文件
├── pyproject.toml             # 依赖（uv/poetry）
└── .env                       # 秘钥（.gitignore）
```

## 下一步

1. `cd ~/brain/30-projects/rQuant && git init`
2. 决定用 uv 还是 poetry 管依赖
3. 先走 Week 1：Tushare 接入 + DuckDB 存日线数据
4. 克隆 `myhhub/stock` 和 `qstock` 源码先读一遍，判断是否 fork 更划算

## 开放决策（待想清楚）

- [ ] 是 fork `myhhub/stock` 改还是从零写？**倾向从零但参考它**
- [x] 筛选规则用 Python 函数还是 YAML DSL？**最终路径**：MVP（Week 3b）用 Python 函数积木 → Week 7 加 NL 输入（LLM → 积木调用）→ Week 8 支持通达信选股公式。**不走 YAML DSL**，因为 NL + 通达信代码已覆盖配置化需求
- [ ] 告警频率怎么防刷屏（去重 / 冷却期）？
- [ ] 多因子综合打分还是条件硬筛选？**MVP 先硬筛选，迭代中加打分**
- [x] 移动端通知用哪条通道？**最终选 PushDeer**（cc2im 受限于微信 token；PushDeer iOS/Mac 双端 + 云端友好）

## 风险提示

- **爬虫源稳定性**：Ashare 依赖新浪/腾讯接口，源站改版即失效 → 必须多源容灾
- **Tushare 积分通胀**：历史上涨过，留预算余量
- **盘中高频调用反爬**：备选池 >100 只或轮询 <2s 易被限流 → 控池大小 + 降频
- **不做实盘的边界要守住**：本项目明确不做下单逻辑，避免盲目扩张功能
