# Week 7.5 — NL 选股节点画布设计

> 状态：设计已锁定，待实施
> 日期：2026-05-11
> 关联：v0.12.0 (Week 7 NL 选股 Stage Cards) → Week 7.5 (本文档) → Week 7.6 (Pool 完整模型)

## 1. 背景

v0.12.0 上线 Stage Cards 形态的 NL 选股后，用户反馈核心痛点：

> "页面布局占地太大，交互流程也不是想要的，还是感觉应该是个画布，然后在上边添加一个一个'池子'给池子配规则"

→ Stage Cards 把"一个 plan 里多个 stage 分组"当主线，跟用户实际心智模型（多个 pool 节点 + depends_on 连接）不匹配。

## 2. 用户心智模型（业务流程对齐）

用户描述的目标业务流程：

1. **筛选器1**（trigger = 每天收盘时）：扫全市场命中"首板/非一字板/过滤 ST/…"等规则的标的
2. **筛选器2-1**（trigger = 交易时段）：接在筛选器1 后边，对前一天 1 的结果列表做盘中实时监控，命中条件 → 触发 action
3. **筛选器2-2**（trigger = 收盘后）：也接 1 后边，对 T 日 1 的结果在 T+1 应用进一步规则，命中进 Pool 2-2
4. **action 列表**：包含"清空当前池子"，让 Pool 1 是每日刷新、Pool 2 是累积

→ 抽象出 **Pool 五元组统一模型**：

| 属性 | 选项 |
|---|---|
| **trigger** | 收盘时 / 交易时段 / 收盘后 / 手动 / 定时 |
| **input** | 全市场 / 上游 pool[]（带 lookback_days） |
| **rules** | 条件列表（已有，rules CRUD） |
| **actions** | 推送告警 / 加入指定池 / 清空池 / 移除标的 |
| **storage** | 替换 / 累加 / 滚动 N 天 |

"筛选器 / 监控器 / 后置筛选器" 不是不同类型，是同一 Pool 抽象在 trigger/action 上的不同配置。

## 3. Week 7.5 Scope（α 阶段 ≠ β 完整模型）

完整 Pool 五元组模型涉及调度 / action 引擎 / 数据迁移，工期 3-4 周。Week 7.5 只做画布 UI + 现有规则模型，β 完整版推迟到 **Week 7.6**。

### 3.1 包含

- streamlit-flow 节点画布替代 Stage Cards
- 现有 `PRESET_SCREENS` + `user_presets/*.json` 渲染为画布节点
- 节点间 edge 表达 `depends_on`（含 `lookback_days` 属性）
- 节点选中后的规则 CRUD（增/删/改/查/排）
- NL 修改规则（单条修改 + 批量修改）
- 命中标的预览（含改动 pending 状态的 diff）
- 画布级新建 pool（空白处右键）

### 3.2 不包含（推迟到 Week 7.6）

- 多 trigger 类型（除现有"每日收盘"外）
- action 列表（除现有"推送告警"外）
- storage strategy（累加 / 滚动）
- 数据迁移（pool schema 加新字段）
- 凯心-style 大盘情绪监控（依赖 trigger=交易时段）

### 3.3 内部三阶段（A → B → C）

| 阶段 | 范围 | 估算 |
|---|---|---|
| **A spike** | streamlit-flow 验证：渲染 Pool1→Pool2 只读节点+连线，证明库稳定可用 | 1-2 天 |
| **B 只读** | 全部 user_presets + builtin 渲染为画布；节点选中右侧面板显示规则 + per-rule diagnostic + 命中标的 | 3-5 天 |
| **C 可编辑** | 规则 CRUD + edge CRUD + 空白处右键新建 pool + NL 改 pool | 1-1.5 周 |

**A 决策门：** spike 通过 → 走 C；不通过 → 回退 `streamlit-agraph`，节点内只显示文字摘要，per-rule 诊断挪到详情面板（仍能走 B）。

## 4. UI 交互模型（B4 + CRUD + diff 预览）

### 4.1 布局

- 画布左 ~65%，右侧详情面板 ~35%
- 画布固定，节点紧凑（不内联展开）
- 选中节点 → 右侧面板更新

### 4.2 右侧面板分层（从上到下）

```
┌─ Pool Header ─────────────────────┐
│ Pool 名称 · trigger badge         │
│ 命中 N 只 · 更新时间               │
├─ Pending Banner（有未保存改动时）─┤
│ ⚠ N 处改动 · 23 → 15  [撤销][保存] │
├─ 规则 Section (Accordion 展开) ───┤
│ ▼ 规则（5）  287→23     [+ 加规则]│
│ ⋮ MA(20)>MA(60)  ████ 287  ✎  ×  │
│ ⋮ KDJ_J<20       ██   142  ✎  ×  │
│ ...                               │
├─ NL Prompt ──────────────────────┤
│ 💬 用自然语言修改这个 pool        │
│ [输入框] 例: 加量比>1.5; 删KDJ   │
├─ 命中标的（含 diff 预览）─────────┤
│ 代码 | 名称 | 价 | 涨跌 | 状态    │
│ 600036 | 招行 | ... | 保留        │
│ ̶3̶0̶0̶7̶5̶0̶ ̶|̶ ̶宁̶时̶ ̶|̶ ̶.̶.̶.̶ ̶|̶ ̶剔̶除̶        │
│ ...                               │
│ [加入 watchlist] [导出 CSV]       │
└──────────────────────────────────┘
```

### 4.3 关键交互

| 动作 | 触发 | 反馈 |
|---|---|---|
| 查看 pool 详情 | 点击节点 | 右侧面板显示该 pool 全部信息 |
| 改规则参数 | 行尾 ✎ → inline 表单 | 进入 pending：banner 显示改动数、规则诊断 count 变橙、命中标的列表 diff（剔除划线 / 新增高亮） |
| 删规则 | 行尾 × | pending 状态，同上 |
| 加规则 | `+ 加规则`弹窗 | 3 路径：模板 / NL / 手写表达式 |
| NL 批量改 | 底部输入框 | LLM 解析 → 多步 diff 预览 → 一键保存全部 |
| 改规则顺序 | 行首 ⋮ 拖拽 | 重排影响 per-rule diagnostic 累加方向 |
| 保存 | banner 点保存 | 写入 `user_presets/<pool>.json`，pipeline 重跑 |
| 撤销 | banner 点撤销 | 还原所有 pending 改动 |

### 4.4 画布级操作（Week 7.5 包含）

- **新建 pool**：空白处右键 → 弹窗 "+ 加 Pool"（类似加规则的 3 路径选择）
- **edge 编辑**：从节点边缘拖拽到目标节点 → 创建 edge；选中 edge 浮窗改 `lookback_days`；Del 删除

## 5. 持久化方案

**方案 B：pool 独立文件 + canvas 单独存 layout**

```
user_presets/<pool_id>.json        # 每个 pool 独立（兼容 v0.12.0）
canvas/<canvas_name>.json          # 画布只存 layout + edges
  {
    "nodes": [{"pool_id": "n-shape-pool1", "x": 100, "y": 200}, ...],
    "edges": [{"from": "n-shape-pool1", "to": "n-shape-pool2", "lookback_days": 2}, ...]
  }
```

**理由：**

- 兼容现有 `user_presets/*.json`（v0.12.0 落库），零破坏性变更
- Pool 仍是一级公民，可独立被 pipeline.py / monitor.py 调用
- Canvas 只是 view 层（节点摆放 + 关系），跟 pool 业务语义解耦
- Week 7.6 给 pool schema 加 trigger/actions/storage 字段时，canvas 文件不动

## 6. Stage Cards 去留

**暂时保留**，提供 view 切换：Canvas / Stage Cards。等 Week 7.6 完整 Pool 模型稳定运行一段时间后再考虑去除。

## 7. 与其他工作的关系

| 任务 | 关系 |
|---|---|
| Week 8（通达信解析） | 串行：先 7.5 → 7.6 → 8 |
| Week 7.6（Pool 完整模型） | 直接续接，重用 Week 7.5 画布作 UI |
| 凯心-style 大盘情绪监控（TODO） | 依赖 Week 7.6 的 trigger=交易时段 + action |
| 前后端分离 GUI（TODO） | 远期，本方案先在 Streamlit 内完成 |

## 8. 风险与开放点

- **streamlit-flow 稳定性**：未知，A spike 验证；不行回退 agraph + B 阶段降级
- **画布性能**：当 pool 数 > 20，节点 + 实时 diagnostic 渲染压力，A 阶段实测
- **lookback_days vs trigger 的责任划分**：当前放在 edge 上，Week 7.6 加 trigger 后可能需要重新审视（trigger 决定何时跑，edge 决定取上游哪天数据，两者解耦）
- **NL 改 pool 的 prompt 工程**：v0.12.0 的 NL → rules 走通了，但"修改"语义跟"新建"不同（要 diff、要保留未改动规则），需要单独 prompt 调优

## 9. 验收标准

| 阶段 | 验收 |
|---|---|
| A 结束 | 浏览器看到 Pool1 → Pool2 的只读节点 + 连线；streamlit-flow 在当前 Streamlit 版本无 console error / refresh loop |
| B 结束 | 所有 `user_presets/*.json` 都渲染为节点，点击节点右侧面板显示规则列表 + per-rule diagnostic 漏斗 + 命中标的列表 |
| C 结束 | 能在画布上完成端到端流程："空白处右键新建 pool → NL 输入需求 → LLM 写规则 → 看命中预览 → 改阈值看 diff → 保存到 user_presets" |

## 10. 实施次序建议

1. 创建 feature branch `feat/week7-5-canvas`（PR-only，不直推 main）
2. A spike（独立 PR）→ 决策门
3. B 只读（独立 PR）
4. C 可编辑分若干小 PR：规则 CRUD / edge CRUD / 新建 pool / NL 修改
5. 每个 PR 走 `scripts/deploy.sh` 部署链路

## 附：mockup 速描

mockup HTML 已废弃（跨日 /tmp 清理）。关键设计点见 §4.2 ASCII 草图。需要重渲染时按 §4 描述用 frontend-design skill 生成。
