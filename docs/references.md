---
title: rQuant 开源参考项目
created_at: 2026-04-15
tags: [quant, open-source, reference]
---

# 开源参考项目

按与 rQuant 场景（**条件筛选 + 监控 + 告警，不做实盘**）的适配度分层。

## 第一档｜贴近 rQuant 场景（优先读源码）

### myhhub/stock ★★★★★
- **地址**：https://github.com/myhhub/stock
- **特点**：数据获取 + 计算指标 + 筹码分布 + 形态识别 + 综合选股 + 选股策略 + 回测验证 + 股票自动交易，支持 PC 及移动设备
- **适配度**：**最接近 rQuant 要做的事**
- **学习重点**：
  - 它如何分层组织代码
  - 选股策略的表达方式（函数 vs DSL）
  - 数据更新的调度设计
  - 如何处理停牌/除权等边界情况
- **决策**：**先 clone 读一遍**，判断是否 fork 改

### qstock ★★★★★
- **地址**：https://github.com/tkfy920/qstock
- **出品**：Python 金融量化公众号
- **模块**：数据获取（data）、可视化（plot）、选股（stock）、回测（backtest）
- **选股模块特色**：RPS、MM 趋势、财务指标、资金流模型
- **学习重点**：
  - 选股模块的因子设计
  - 简洁 API 的接口抽象

### daily_stock_analysis ★★★★★
- **地址**：https://github.com/ZhuLinsen/daily_stock_analysis
- **特点**：LLM 驱动的 A/H/美股智能分析器；多数据源 + 实时新闻 + LLM 决策仪表盘 + 多渠道推送；**零成本定时运行**
- **推送渠道**：企业微信、飞书、Telegram、Discord、Slack、邮箱
- **适配度**：**推送仪表盘部分的参考价值最大**
- **学习重点**：
  - 多渠道通知的抽象设计
  - 定时任务的组织

### QuantDinger ★★★★
- **地址**：https://github.com/brokermr810/QuantDinger
- **特点**：本地部署 AI 量化平台；Docker 一键启动；AI Agent 研究团队 + 本地 TradingView-like 可视化
- **支持**：A 股、港股、美股、加密、外汇、期货
- **学习重点**：
  - 本地部署的完整方案
  - AI Agent 如何接入量化流程

### aiagents-stock ★★★★
- **地址**：https://github.com/oficcejo/aiagents-stock
- **特点**：多 AI 智能体盯盘系统；模拟证券分析师团队；龙虎榜跟踪；板块预警；实时关键点位监测 + 警报；预留 miniqmt 接口
- **学习重点**：
  - 多智能体协同分析的架构
  - 关键点位的告警设计

## 第二档｜框架级（搭骨架不搭业务）

### vnpy
- **地址**：https://github.com/vnpy/vnpy
- **定位**：交易为主，框架最成熟
- **参考价值**：事件驱动架构

### QUANTAXIS
- **地址**：https://github.com/yutiansut/QUANTAXIS
- **定位**：纯本地量化，数据/回测/实盘分层清晰
- **参考价值**：分层设计

### Qlib（微软）
- **地址**：https://github.com/microsoft/qlib
- **定位**：AI 量化研究框架（11.4k star）
- **参考价值**：
  - 数据管道设计（binary 格式）
  - Alpha 因子库组织
  - **不建议直接用**，它的数据格式锁定

### Hikyuu
- **地址**：https://github.com/fasiondog/hikyuu
- **定位**：C++/Python 极速回测
- **参考价值**：性能极致追求时可参考

### abu（阿布量化）
- **地址**：https://github.com/bbfamily/abu
- **定位**：综合量化平台 + ML
- **参考价值**：传统 + ML 融合

### ai_quant_trade
- **地址**：https://github.com/charliedream1/ai_quant_trade
- **定位**：从学习到实盘一站式平台
- **参考价值**：知识体系组织

## 第三档｜单组件（即插即用）

### easyquotation
- **地址**：https://github.com/shidenggui/easyquotation
- **功能**：实时行情获取（新浪/腾讯/集思录）
- **用途**：如果 Ashare 不够用，这里有更多源

### daban
- **地址**：https://github.com/freevolunteer/daban
- **功能**：盯盘打板策略工具
- **平台**：Windows / Mac / Linux 多平台
- **用途**：打板相关的监控逻辑参考

### khQuant / OSkhQuant
- **地址**：https://github.com/khscience/OSkhQuant
- **功能**：A 股可视化回测
- **用途**：回测可视化参考

### PandaAI QuantFlow
- **地址**：https://github.com/PandaAI-Tech/panda_quantflow
- **定位**：量化 + ML 工作流平台

## 建议的阅读顺序

**第 1 周（启动期）**：
1. myhhub/stock — 看它怎么分层
2. qstock — 看选股模块的 API 设计
3. daily_stock_analysis — 看推送仪表盘怎么搭

**第 2-3 周（搭架子期）**：
- 参考 QUANTAXIS 的数据层分层
- 参考 vnpy 的事件驱动设计（如果将来要扩）

**后续（需要时再看）**：
- Qlib 的因子库组织（需要做多因子时）
- aiagents-stock（加入 AI Agent 时）

## 决策点

**克隆完这些项目读完源码后，要回答一个关键问题**：

> 是 fork 其中一个最接近的版本改，还是从零写？

**倾向**：从零写，但先完整读懂 myhhub/stock 和 qstock 的选股实现。原因：
- 这些项目都是单人项目，代码质量不一
- 从零写能让架构完全按自己的场景来
- 但绝不从零造轮子——**指标计算、数据接入这类工具性代码复用开源**

**反对意见**：
- 从零写耗时长，容易烂尾
- Fork 后删代码比从零写快得多

**决策时机**：Week 1 结束后，数据流打通了再决定。
