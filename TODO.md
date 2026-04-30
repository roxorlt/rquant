# rQuant TODO

> 已完成项目阶段见 `CHANGELOG.md`。当前版本 **v0.12.0**（Week 7 NL 选股）+ **v0.11.3**（backup intraday timer 修复，今日稳定性补丁链 v0.10.1→v0.11.3）。

---

## ⏰ 待观察（已部署，等真实场景验证）

- [ ] **5/1 15:30 第一份 daily-report**（节假日干净跑）
  - 期望：`✅ watchdog: 交易时段 N 次（active=0 skip=N），无告警`
  - 异常：watchdog alert > 0 → v0.10.2 节假日 skip-clean-exit 修复在新 timer 下又翻车
- [ ] **5/6 周二节后第一个完整交易日**
  - monitor 跨午休：日报应见 `monitor: ... 跑足 5h+（含跨午休）` ← v0.10.1 真实回归
  - backup intraday：`latest.duckdb.gz` 应每 5min 微变，本地 sync 拉到接近实时数据 ← v0.11.3 真实验证
  - watchdog 全程：交易时段 ~165 次 active，0 alert ← v0.11.2 真实验证

---

## MVP 收尾（剩 Week 8）

- [x] **Week 7**：Streamlit UI 自然语言输入（v0.12.0，2026-04-30）
- [ ] **Week 8**：通达信选股公式支持（解析器 → MyTT/积木）

## Week 7.5（NL 选股下游优化）

- [ ] 真节点画布 UI 升级：streamlit-flow / react-flow 集成，Stage 升级 Node，DAG 编辑
  - 触发条件：v0.12.0 上线后用户实际使用反馈，确认 Stage Cards 不足
- [ ] LLM-driven 画布操作：用户在画布上"问 LLM"添加节点 / 修改节点
- [ ] preset 保存为子图模板，多 preset DAG 关系可视化

---

## 优化与重构

- [ ] **Pool 1 / Pool 2 阈值持续观察调优**（看一段时间 monitor_event 命中率与噪音比再定）
- [ ] **`_count_trading_days_since` 重构**：monitor.py 与 pipeline.py 重复实现，合并到 `DuckDBStore.count_trading_days_between(start, end)`
- [ ] **Dashboard `_market_phase_now()` 午休 idle 三态**：v0.10.1 接入后小遗留——lunch 时段（11:30-13:00）monitor 仍是 active（在 lunch sleep 循环），badge 显示绿色。其实想改成「lunch · idle」灰色让看板更准。不紧急（cosmetic），跟 Week 7.5 画布升级一起改顺手
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
