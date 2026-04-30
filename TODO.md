# rQuant TODO

> 已完成项目阶段见 `CHANGELOG.md`。当前版本 v0.8.0（云端 systemd 调度 + dashboard + Backup HTTP API）。

---

## P0：风险控制名单接入（"430 黑名单"）

实现完成于分支 `feat/risk-blacklist`（待 merge）。

- [x] PDF 解析（pypdf）+ 代码标准化（补前导 0 + 自动加 SH/SZ/BJ）+ 多类别合并
- [x] DuckDB `risk_blacklist` 表 + import / load / filter / annotate API
- [x] Pipeline 新推荐**剔除**（`run_daily_pipeline` 落库前 filter）
- [x] Monitor 已持仓**保留+标签**（subject 加 `[430黑名单]` 前缀 + body ⚠️ 类别行）
- [x] Dashboard 黑名单状态 Section + Pool 2 表 黑名单 列 + 过期红色提醒
- [x] CLI `rquant blacklist {import,list,check,remove}`
- [x] 单测 + pipeline 集成测（22+2 用例）

**剩余手动验收（merge 后）**：
1. 在生产 DuckDB 上执行 `rquant blacklist import ~/Downloads/风险控制名单.pdf`
2. 跑下一次 daily 流水线，确认黑名单内的票被 filter 掉（log 会有 `黑名单过滤剔除 N 只 → [...]`）
3. 打开 dashboard 看 Section 9 显示 "430黑名单 · 147 只 · 剩 365d"

---

## MVP 收尾（剩 Week 7 / Week 8）

- [ ] **Week 7**：Streamlit UI 的**自然语言输入**（Dashboard 框架 v0.7.0 已上，NL → 积木调用尚未做）
  - 输入示例："找出连续 3 天放量上涨且 MA5 上穿 MA20 的股票"
  - LLM 解析 → 调用现有 rule registry 积木 → 结果回 dashboard
- [ ] **Week 8**：通达信选股公式支持（解析器 → MyTT/积木）

---

## 优化与重构

- [ ] **Pool 1 / Pool 2 阈值持续观察调优**（看一段时间 monitor_event 命中率与噪音比再定）
- [ ] **`_count_trading_days_since` 重构**：monitor.py 与 pipeline.py 重复实现，合并到 `DuckDBStore.count_trading_days_between(start, end)`
- [ ] **Monitor 弹窗 UX**：当前 `check_exits` 逐个标的弹原生 macOS dialog，三个问题：
  1. 可视化不足：纯数字（body / 档位 / 止损价），应画日 K 样式 + 档位线 + 当前位置
  2. 进度焦虑：连续多个弹窗没有「N/M」进度提示
  3. 云端不适用：腾讯云上原生 dialog 跑不起来，必须改为「批量列表 + 网页一次性勾选 → 提交」
  - 实施时机：跟 Week 7 Streamlit UI 一起做，框架建好后顺势改成网页交互

---

## 后续（未排期）

- [ ] 盘中分时明细（akshare 1min bars）
- [ ] tick 数据存储（watchlist 50 只，约 1.5-2GB/年）
- [ ] 前后端分离 GUI 产品化（rule registry + FastAPI + 前端，nginx 反代架构 v0.8.0 已铺好）
