# 脉搏 Push 手机端分块排版 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 把 30 分钟脉搏正文从超长单行列表改成适合手机扫读的分节 Markdown。

**Architecture:** 只修改 `render_pulse()` 的纯渲染层。计算模型、槽位增量、数量上限、
通知 scene、数据落盘和午间战报保持不变。

**Tech Stack:** Python 3.11/3.12、Pydantic、pytest、Markdown、PushDeer。

---

### Task 1: 固化手机端分节契约

**Files:**
- Modify: `tests/unit/test_midday_briefing.py`

**Step 1: Write the failing test**

构造同时包含新晋涨停、题材热度和放量异动的 `PulseView`，断言正文包含：

```markdown
## 市场温度
- 涨停：72（+14） ｜ 炸板：19（+6） ｜ 跌停：2
- 上涨：5022 ｜ 下跌：434

## 新晋涨停
- 京投发展 ｜ 银发经济

## 题材热度
- AI硬件：10板（+3）

## 放量异动新增
- 上海凯宝：量比 1.26
```

并断言旧的 `新晋涨停：...` 单行格式不存在。

**Step 2: Run test to verify it fails**

Run: `pytest -q tests/unit/test_midday_briefing.py::TestU7Rendering::test_pulse_groups_sections_for_mobile`

Expected: FAIL，因为旧渲染没有分节标题和逐项 bullet。

### Task 2: 实现最小渲染调整

**Files:**
- Modify: `src/rquant/midday_briefing.py:892`
- Modify: `CHANGELOG.md`
- Modify: `pyproject.toml`
- Modify: `src/rquant/__init__.py`

**Step 1: Write minimal implementation**

用四个可选分节组织原有字段；使用中文全角括号展示增量，标题继续使用紧凑摘要。

**Step 2: Run focused tests**

Run: `pytest -q tests/unit/test_midday_briefing.py`

Expected: PASS。

**Step 3: Run quality checks**

Run: `ruff check src/rquant/midday_briefing.py tests/unit/test_midday_briefing.py`

Expected: PASS。

### Task 3: 受控发布

**Files:**
- Modify: `CHANGELOG.md`

**Step 1: Publish**

提交 `fix(notify): format pulse alerts for mobile`，创建 ready PR，等待 Python 3.11/3.12 CI
全绿后 squash merge。

**Step 2: Tag and schedule**

创建 annotated `v0.26.9`，确保 peeled tag 指向合并后的 `origin/main`，再把 15:15 一次性
发布任务从 `v0.26.8` 更新为 `v0.26.9`。生产部署仍只走
`scripts/deploy-production.sh --target v0.26.9`。
