# rQuant TODO

> 已完成项目阶段见 `CHANGELOG.md`。当前版本 **v0.12.1**（nl-screen 只读 DuckDB hotfix）。

---

## 🔥 P0 — 5/13 复盘暴露的三个 bug（代码已修，等真实场景验证）

> 详细时间线 / 根因 / 修复方案：[docs/incidents/2026-05-13-may1-and-may6-failures.md](docs/incidents/2026-05-13-may1-and-may6-failures.md)

- [x] **Bug C · backup intraday 从未真跑过** → PR #8 `fix/backup-timer-deploy-and-verify` merged 2026-05-13；云端热修已生效（手动 cp + restart timer，5/13 17:30 + 5/14 09:00 起每 5min）
- [x] **Bug A · daily-report 撞 DuckDB 写锁** → PR #10 `fix/daily-report-readonly-duckdb` merged 2026-05-13（修复时发现 daily-report 实际不写 db，方案简化为 `read_only=True` 一行 fix，没走最初设想的 snapshot 包装器）
- [x] **Bug B · watchdog 自愈失败** → PR #9 `fix/watchdog-self-heal-sudo` merged 2026-05-13
- [x] **Task #8 · 本地 sync 假阳性 OK** → PR #11 `chore/sync-stale-detection` merged 2026-05-13

### ⏰ 明日 5/14 真实场景验证

- [ ] **09:00+ backup intraday 真跑**：`stat ~/rquant/backup/latest.duckdb.gz` mtime 在 5min 内；`journalctl -u rquant-backup.service --since "today 09:00"` 见 ~7 次（9:00→9:30 期间）
- [ ] **15:30 daily-report**：`journalctl -u rquant-daily-report.service --since "today 15:30"` 无 IOException，正常 exit 0，PushDeer 收到日报
- [ ] **本地 mac sync stale**：本地下次 sync log 显示 `(snapshot_at=...)` 每 5min 变化，不再报 stale；如果 backup intraday 故障会自动 PushDeer
- [ ] **monitor 真挂时 watchdog 自愈**：故意 `sudo systemctl stop rquant-monitor.service` 验证下次 watchdog（2min 内）能 sudo systemctl start 回来；或等真挂被动验证

---

## MVP 收尾（剩 Week 8）

- [x] **Week 7**：Streamlit UI 自然语言输入（v0.12.0，2026-04-30）
- [ ] **Week 8**：通达信选股公式支持（解析器 → MyTT/积木）

## Week 7.5（NL 选股节点画布）

> 设计文档：[docs/plans/2026-05-11-week7-5-canvas-design.md](docs/plans/2026-05-11-week7-5-canvas-design.md)
> 内部三阶段 A→B→C 串行：A spike streamlit-flow → B 只读画布 → C 可编辑（规则 CRUD + edge + 新建 pool + NL 改）

- [ ] **A spike**（1-2 天）：streamlit-flow 渲染 Pool1→Pool2 只读节点 + 连线，验证库稳定可用；不行回退 streamlit-agraph
- [ ] **B 只读画布**（3-5 天）：全部 user_presets + builtin 渲染为节点；点击节点右侧面板显示规则 + per-rule diagnostic + 命中标的
- [ ] **C 可编辑**（1-1.5 周）：规则 CRUD（inline edit + delete + drag reorder + 3-path add）+ edge CRUD（lookback_days）+ 空白处右键新建 pool + NL 单条/批量改 pool（含 diff 预览）

## Week 7.6（Pool 完整模型 — 推迟到 7.5 完成后）

> Week 7.5 只做画布 UI，完整 Pool 五元组模型推到 7.6

- [ ] Pool 模型扩展：trigger（收盘时/交易时段/收盘后/手动/定时）+ input + rules + actions + storage
- [ ] action 引擎：推送告警 / 加入指定池 / 清空池 / 移除标的
- [ ] storage strategy：替换 / 累加 / 滚动 N 天
- [ ] 数据迁移：现有 user_presets/*.json → 加默认 trigger/actions/storage 字段
- [ ] 凯心-style 大盘情绪监控（依赖 trigger=交易时段 + 多 action）从「后续未排期」移到这里

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

- [ ] **凯心-style 大盘情绪监控**：14:00 vs 14:55 数据对比触发次日策略提醒
  - **地板数量**（跌停/接近跌停）
    - 增加 +50%（如 6→9+）：先走「次日接力」（恐慌见底，超跌反弹）
    - 减少 -50%（如 6→3-）：可走「次日惯性冲高」
  - **涨停（zt）数量**
    - 增加 +30%（如 10→13）：情绪转暖，可参与
  - 实现要点：14:00 / 14:55 各采一次快照（盘中数据源），算 delta，触发 PushDeer
- [ ] 盘中分时明细（akshare 1min bars）
- [ ] tick 数据存储（watchlist 50 只，约 1.5-2GB/年）
- [ ] 前后端分离 GUI 产品化（rule registry + FastAPI + 前端，nginx 反代架构 v0.8.0 已铺好）
