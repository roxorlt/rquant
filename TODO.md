# rQuant TODO

> 已完成项目阶段见 `CHANGELOG.md`。当前版本 v0.10.0（Upload HTTP API + 黑名单部署完成）。

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
