# 爆量 Push 移动端分组排版 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 在不改变爆量判定、字段、聚合数量和通知通道的前提下，把每只标的从单行字段堆叠改为适合手机阅读的分组 Markdown。

**Architecture:** 只修改 `build_surge_messages()` 的纯文本渲染层。每只标的用标题展示状态、代码和名称，题材独占一行；涨幅/涨停空间、累计比/累计额、分钟方向分别分行，可选增量门开启时再增加一行。尾注、折叠和 PushDeer 路由保持不变。

**Tech Stack:** Python 3.11/3.12、pytest、Markdown、PushDeer。

---

### Task 1: 固化手机友好排版契约

**Files:**
- Modify: `tests/unit/test_surge_watch.py`

**Step 1: Write the failing test**

新增单标的断言，要求正文包含独立的标的标题行、指标行和空间行，并禁止旧的单行堆叠格式。

**Step 2: Run test to verify it fails**

Run: `pytest -q tests/unit/test_surge_watch.py::TestU10Push::test_message_groups_each_stock_for_mobile`

Expected: FAIL，因为当前所有字段仍在同一行。

### Task 2: 实现最小排版调整

**Files:**
- Modify: `src/rquant/surge_watch.py`
- Modify: `CHANGELOG.md`

**Step 1: Write minimal implementation**

将每个标的渲染为：

```markdown
## 300001.SZ 机器人A
**题材**：人形机器人
- 涨幅：+8.3% ｜ 距涨停：4.5%
- 累计比：4日 2.7× ｜ 累计额：1.20亿
- 方向：1分钟 +0.46% ｜ 外/内≈2.34×
```

临近涨停或已封板图标保留在标的标题前；可选增量门开启时单独成行。

**Step 2: Run focused and full module tests**

Run: `pytest -q tests/unit/test_surge_watch.py::TestU10Push`

Run: `pytest -q tests/unit/test_surge_watch.py`

Expected: PASS。

**Step 3: Run quality checks**

Run: `ruff check src/rquant/surge_watch.py tests/unit/test_surge_watch.py`

Expected: PASS。

### Task 3: 受控发布

**Files:**
- Modify: `CHANGELOG.md`

**Step 1: Merge and tag**

CI 在 Python 3.11/3.12 全绿后 squash merge，创建下一个 annotated SemVer tag，确保 tag 指向合并后的 `origin/main`。

**Step 2: Deploy after market**

15:10 后仅通过：

```bash
bash scripts/deploy-production.sh --target <exact-tag>
```

部署器负责 preflight、白名单服务重启、二次 preflight 和失败回滚。禁止在午休绕过交易时段保护。
