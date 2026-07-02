---
title: A 股数据源竞品矩阵
created_at: 2026-04-15
tags: [quant, data-source, a-shares, research]
---

# A 股数据源竞品矩阵

> 本文档保留了 rQuant 项目启动前做的数据源调研成果（2026-04-15）。
> 用于后续数据源切换/补充时的参考。

## 调研范围

覆盖 19 个主流产品，分五大类：
- 开源免费库（个人量化的基本盘）
- 量化平台（自带数据 + IDE + 回测）
- 商业终端（Wind / iFinD / Choice）
- 券商实盘接口（QMT / PTrade）
- 开源框架（Qlib / Hikyuu 等）

## 额外关注的字段（标准竞品模板之外）

- **数据覆盖**：日线/分钟线/Tick/L2/财务/板块概念/资金流/龙虎榜/研报/宏观
- **接口形式**：Python SDK / HTTP API / 本地客户端（Windows only）/ Web IDE
- **实时性**：延时几毫秒？pull 还是 push？是否支持全市场推送
- **历史深度**：日线/分钟线/Tick 各自能回溯到哪一年
- **调用限制**：QPS / 每日限额 / 并发 / 反爬触发阈值
- **稳定性风险**：官方 API 限流 vs 爬虫被封 vs 托管宕机
- **是否支持实盘**
- **合规性**：商业用途 vs 仅限个人研究
- **本地化需求**：是否必须 Windows

## 主矩阵

| 产品 | 类型 | 官网 | 个人收费 | 开通门槛 | 数据覆盖 | 实时性 | 接口 | rQuant 推荐度 |
|---|---|---|---|---|---|---|---|---|
| <img src="https://www.google.com/s2/favicons?domain=tushare.pro&sz=32" width="20"> [Tushare Pro](https://tushare.pro) | 数据 API | tushare.pro | 500 元 = 5000 积分覆盖大部分 A 股日线需求；分钟/港美股另付 | 注册+完善资料 120 积分起步 | 日/分钟/财务/资金流/龙虎榜/板块/宏观 | 日级 T+1；实时 tick 为爬虫接口 | Python SDK | ★★★★★ **主数据源** |
| <img src="https://www.google.com/s2/favicons?domain=akshare.akfamily.xyz&sz=32" width="20"> [AKShare](https://akshare.akfamily.xyz) | 数据 API（爬虫聚合） | akshare.akfamily.xyz | 完全免费 | 无 | 最广（行情+财务+宏观+另类+海外） | ~500ms 延迟 | Python SDK / HTTP | ★★★★★ **兜底源** |
| <img src="https://www.google.com/s2/favicons?domain=baostock.com&sz=32" width="20"> [Baostock](https://www.baostock.com) | 数据 API | baostock.com | 完全免费 | 无需注册 | 日/分钟/财务/指数，无 Tick | 日级 T+1 | Python SDK | ★★★★ 可选 |
| <img src="https://www.google.com/s2/favicons?domain=github.com&sz=32" width="20"> [Ashare](https://github.com/mpquant/Ashare) | 爬虫库 | github.com/mpquant/Ashare | 完全免费 | 无 | 仅行情（日/分时/分钟） | 准实时（新浪+腾讯双核） | Python 单文件 | ★★★★ **实时监控** |
| <img src="https://www.google.com/s2/favicons?domain=github.com&sz=32" width="20"> [adata](https://github.com/1nchaos/adata) | 爬虫库（多源融合） | github.com/1nchaos/adata | 完全免费 | 无 | 行情+概念+交易数据 | 准实时 | Python SDK | ★★★ 备选 |
| <img src="https://www.google.com/s2/favicons?domain=github.com&sz=32" width="20"> [efinance](https://github.com/Micro-sheep/efinance) | 爬虫库 | github.com/Micro-sheep/efinance | 完全免费 | 无 | 基金/股票/债券/期货（东财） | 准实时 | Python SDK | ★★ 作者已转新项目 |
| <img src="https://www.google.com/s2/favicons?domain=github.com&sz=32" width="20"> [mootdx](https://github.com/bopo/mootdx) | 爬虫库（通达信协议） | github.com/bopo/mootdx | 完全免费 | 无 | 历史分钟线（8-12 年）、Tick | — | Python SDK | ★★★★ **分钟补刀** |
| <img src="https://www.google.com/s2/favicons?domain=joinquant.com&sz=32" width="20"> [聚宽 JoinQuant](https://www.joinquant.com) | 量化平台 + JQData SDK | joinquant.com | JQData 每日 100 万条免费；平台 Pro 付费 | 注册+申请试用 | Tick（20 年）/日/分钟/财务/因子 | 实时可订阅 | Web IDE + Python SDK | ★★★ 备选 |
| <img src="https://www.google.com/s2/favicons?domain=ricequant.com&sz=32" width="20"> [米筐 RiceQuant](https://www.ricequant.com) | 量化平台 + RQData | ricequant.com | 试用 14 天；年费数千起 | 申请审批 | 日/分钟/Tick/期货/期权/可转债 | 实时 Tick | Web IDE + RQSDK | ★★ rQuant 不需要 |
| <img src="https://www.google.com/s2/favicons?domain=myquant.cn&sz=32" width="20"> [掘金 MyQuant](https://www.myquant.cn) | 量化平台（含终端） | myquant.cn | 免费版够用（限 50 个实时标的） | 注册下载客户端 | 上市以来日线/10 年分钟/1 年 Tick | 免费订阅 50 标的 | Windows 客户端 + Python SDK | ★ **Mac 不适用** |
| <img src="https://www.google.com/s2/favicons?domain=bigquant.com&sz=32" width="20"> [BigQuant](https://bigquant.com) | AI 量化平台 | bigquant.com | 免费 + 付费会员 | 注册 | AI 因子/日线/财务 | 准实时 | Cloud IDE | ★★ 不匹配 |
| <img src="https://www.google.com/s2/favicons?domain=uqer.datayes.com&sz=32" width="20"> [优矿 UQER](https://uqer.datayes.com) | 量化平台（通联） | uqer.datayes.com | 仍开放免费账户 | 注册 | 400+ 因子/日/分钟/财务 | 实时 | Jupyter Web | ★★ 平台老化 |
| <img src="https://www.google.com/s2/favicons?domain=quantapi.eastmoney.com&sz=32" width="20"> [东方财富 Choice API](https://quantapi.eastmoney.com) | 商业终端 API | quantapi.eastmoney.com | ¥38,720/年（推广价 ¥6,520/年） | 注册+客服开通 | 接近 Wind 的全量 | 实时 | Python/C++/C# SDK | ★★★ 若预算允许 |
| <img src="https://www.google.com/s2/favicons?domain=quantapi.10jqka.com.cn&sz=32" width="20"> [同花顺 iFinD](https://www.51ifind.com) | 商业终端 | 51ifind.com | ¥8,800-28,000/年 | 个人试用，正式需申请企业 | 全量 | 实时 | iFinDPy 等 | ★★ 流程繁 |
| <img src="https://www.google.com/s2/favicons?domain=wind.com.cn&sz=32" width="20"> [Wind 万得](https://www.wind.com.cn) | 商业终端旗舰 | wind.com.cn | ¥39,800/年（仅机构） | 机构实名 | 行业最全 | 实时 | WindPy 等 | ★ 不可得 |
| <img src="https://www.google.com/s2/favicons?domain=thinktrader.net&sz=32" width="20"> [迅投 QMT / miniQMT](https://dict.thinktrader.net) | 券商实盘+数据 | dict.thinktrader.net | 开户后免费；门槛 10-50 万 | 券商开通 | 全周期 + 部分 L2 | 实时订阅 | Python xtquant + Windows 客户端 | ★ **Mac 不适用** |
| <img src="https://www.google.com/s2/favicons?domain=hs.net&sz=32" width="20"> [恒生 PTrade](https://www.hs.net) | 券商实盘+策略托管 | hs.net | 开户后免费 | 券商开通 | 全周期 | 实时 | Python + VBA | ★ 不做实盘 |
| <img src="https://www.google.com/s2/favicons?domain=github.com&sz=32" width="20"> [Qlib（微软）](https://github.com/microsoft/qlib) | AI 量化框架 | github.com/microsoft/qlib | 开源免费 | 无 | 框架本身不含数据 | — | Python | ★★★ 可参考架构 |
| <img src="https://www.google.com/s2/favicons?domain=github.com&sz=32" width="20"> [Hikyuu](https://github.com/fasiondog/hikyuu) | 极速量化框架 | github.com/fasiondog/hikyuu | 开源免费 | 无 | 可对接通达信本地 | — | C++/Python | ★★ 重量级 |

## 对 rQuant 的选型决策

### 日线数据（主）
**Tushare Pro ¥500/年 5000 积分**
- 覆盖几乎所有 A 股日线接口 + 资金流 + 龙虎榜
- 风险：积分通胀，留预算余量
- 兜底：AKShare

### 实时监控（盘中）
**Ashare（新浪/腾讯双核爬取）**
- 新浪/腾讯公开接口，支持 5 档盘口
- 准实时，约 500ms-3s 延迟
- 池子 ≤ 50 只 + 轮询 3-5s 不会被封
- 兜底：adata（多源 + 代理）

### 历史分钟补刀
**mootdx（通达信协议 Mac 可用）**
- 8-12 年分钟线深度，比 Baostock 深
- pytdx 的活跃 fork，维护良好

### 明确不用
- Wind / iFinD：太贵
- Choice API：个人年费 6500+ 还是偏贵
- miniQMT / QMT：**Windows-only，Mac 不适用**
- 掘金 MyQuant：**Windows-only**
- PTrade：托管环境装不了三方库，不做实盘也用不上

## 常见场景与数据源对应

| 场景 | 数据源 | 说明 |
|---|---|---|
| 每日日线更新 | Tushare Pro `daily` | 收盘后 19:00 可拉 |
| 财务三表 | Tushare Pro `income/balancesheet/cashflow` | 季度更新 |
| 资金流 | Tushare Pro `moneyflow` | 2000 积分以上 |
| 龙虎榜明细 | Tushare Pro `top_list` / AKShare | T+1 |
| 板块概念 | AKShare `stock_board_concept_*` | 实时 |
| 集合竞价 | Ashare（9:15-9:25 虚拟开盘价） | 实时 |
| 盘中监控 5 档 | Ashare | 准实时 |
| 历史分钟线（回测） | mootdx | 离线 |
| ETF 申赎清单 | Tushare Pro | 日更 |
| 北向资金 | Tushare Pro `hsgt_top10` | T+1（盘中估算无 L2） |

## rQuant 当前接入状态（2026-07-01）

| 场景 | 当前接入 | 数据表/命令 | 回测是否可无未来函数使用 | 备注 |
|---|---|---|---|---|
| 历史 1 分钟 K 线 | Tushare `stk_mins` | `minute_bar` / `rquant minute-backfill` | 可以 | 用于分钟级 replay、90 日价量分布、盘中放量基准。 |
| 实时最新分钟 K 线 | Tushare `rt_min` | `minute_bar` / `rquant rt-minute-fetch` | 实盘监控可用；历史回测不用它 | 当前权限已验证可批量取多只股票最新分钟。 |
| 实时分钟日累计 | Tushare `rt_min_daily` | `minute_bar` / `rquant rt-minute-daily-fetch` | 只用于当天已发生分钟补齐 | 适合盘中服务重启、漏轮询后补齐单只股票当天 9:30 至当前分钟。 |
| 集合竞价 | Tushare `stk_auction` | `auction_bar` / `rquant auction-backfill` | 可以 | 2025-01-01 起有历史数据；缺失行可用 09:30 分钟 K 做保守 fallback。 |
| 集合竞价 fallback | 09:30 `minute_bar` 合成 | `rquant auction-minute-fallback` | 可以 | 只补 Tushare 集合竞价缺行，不覆盖原始集合竞价。 |
| 日级资金流 | Tushare `moneyflow` | `moneyflow_daily` / `rquant moneyflow-backfill` | 只能做盘后复盘/次日过滤 | 它是日级盘后数据，不能用于当日盘中 B 信号。 |
| 外盘/内盘 | AKShare 腾讯分笔可查当前历史分笔 | 暂未落表 | 大样本历史回测不足 | 当前 `stock_zh_a_tick_tx_js` 无日期参数，不适合作为多年历史回测主源。 |
| 盘中大单净量 | 待定 | 暂无 | 暂不可用 | 需要可靠的盘中订单流/资金流历史源；不能用盘后 `moneyflow` 冒充。 |

## 已知风险与对策

| 风险 | 对策 |
|---|---|
| Tushare 积分通胀 | 预算留 30% 余量；关键接口锁定积分等级 |
| 爬虫源站改版（Ashare/AKShare/adata） | 多源容灾，适配层抽象 |
| 盘中轮询被反爬限流 | 池子 ≤ 50 只，轮询间隔 ≥ 3s |
| 不同源数据对不齐（前复权/后复权） | 统一在适配层做标准化 |
| 停牌/除权/特殊交易日 | 初期手动处理，积累经验后自动化 |
| Mac 生态缺失（QMT / 掘金客户端） | 明确放弃，不做实盘 |

## 原始调研对话

本次调研由 Claude 协助完成（2026-04-15）：
- 首轮识别 20+ 产品
- 深挖核心竞品的定价、门槛、口碑
- 结合 Mac 开发 + 不做实盘的约束做过滤
- 最终输出本矩阵 + rQuant 选型决策
