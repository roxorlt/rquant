# Pulse Theme Ladder Formatting Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 用开盘啦真实多对多成员关系生成可供脉搏与午间战报共用的题材连续梯队，并将两种
PushDeer 正文改为手机易扫读的格式。

**Architecture:** 在 `midday_briefing.py` 保持“DataFrame 输入 → Pydantic view → 纯 Markdown
渲染”的分层。新的 `ThemeLadderSummary` 以 KPL 成员×快照的多对多 join 为唯一题材归属，
同时供 `PulseView` 的 Top5 摘要和 `DigestView` 的连续梯队消费；编排层只负责一次只读副本
连接、槽位帧加载和 fail-soft 降级。

**Tech Stack:** Python 3.11+、pandas、Pydantic、DuckDB 只读副本、pytest、Ruff、Markdown、
PushDeer。

---

### Task 1: 为可复用题材梯队锁定纯计算契约

**Files:**
- Modify: `tests/unit/test_midday_briefing.py`
- Modify: `src/rquant/midday_briefing.py:128-205`
- Modify: `src/rquant/midday_briefing.py:654-761`

**Step 1: 写失败的多对多与连续档位测试**

在 `TestU4BoardLadder` 后新增 fixture：一只当前涨停的 `600001.SH` 同时属于“题材A”和
“题材B”，T-1 `limit_times=2`；再放入题材A的一只首板和题材B的一只二板。断言新的
`compute_theme_ladder_summaries(...)`：

```python
assert [summary.theme for summary in summaries] == ["题材A", "题材B"]
assert [(r.boards, [s.name for s in r.stocks]) for r in summaries[0].rungs] == [
    (3, ["共用龙头"]), (2, []), (1, ["题材A首板"]),
]
assert [s.name for s in summaries[1].rungs[0].stocks] == ["共用龙头"]
```

另加一个同涨停数、同成交额的 fixture，断言题材名升序打破并列，保证排名跨槽稳定。

**Step 2: 运行测试，确认当前代码失败**

Run: `pytest -q tests/unit/test_midday_briefing.py -k 'theme_ladder_multi_membership or theme_ladder_continuous_rungs'`

Expected: FAIL，因为 `ThemeLadderSummary` 和 `compute_theme_ladder_summaries` 尚不存在，且
当前 `theme_map_from_kpl()` 会压缩成单题材。

**Step 3: 写最小 Pydantic 模型与计算函数**

在现有 view 模型附近添加 `ThemeLadderRung(boards: int, stocks: list[LadderStock])` 与
`ThemeLadderSummary(theme, limit_up_count, amount, rungs, slot_series, rank_change)`；其中
`rank_change: int | None` 仅表达名次差，`None` 由渲染器按“首槽/新晋”上下文决定。

实现一个只吃 DataFrame 的 `compute_theme_ladder_summaries(snapshot, prev_limit,
kpl_members, slot_snapshots, *, top_n=_THEME_TOP_N)`：

```python
members = kpl_members[["board_name", "con_code"]].dropna().drop_duplicates()
joined = members.merge(snapshot, left_on="con_code", right_on="ts_code", how="inner")
ranked = grouped_current_limit_ups.sort_values(
    ["limit_up_count", "amount", "board_name"], ascending=[False, False, True]
)
for height in range(highest_board, 0, -1):
    rungs.append(ThemeLadderRung(boards=height, stocks=stocks_at_height.get(height, [])))
```

只对当前涨停股计算 `prev_limit.limit_times + 1`；成员×快照 join 的每一行独立统计，使一票
多题材保留。按每个题材汇总全体成员的快照 `amount`，并只让 `limit_up_count > 0` 的题材
参与排序。`rungs` 必须保留空列表，渲染为“0档”；不在计算层裁剪并列最高板名称。

**Step 4: 运行聚焦测试，确认变绿**

Run: `pytest -q tests/unit/test_midday_briefing.py -k 'theme_ladder_multi_membership or theme_ladder_continuous_rungs'`

Expected: PASS。

**Step 5: 提交计算层检查点**

```bash
git add src/rquant/midday_briefing.py tests/unit/test_midday_briefing.py
git commit -m "feat(midday): add reusable theme ladder summary"
```

### Task 2: 固化脉搏 Top5 的龙头与排名变化

**Files:**
- Modify: `tests/unit/test_midday_briefing.py`
- Modify: `src/rquant/midday_briefing.py:141-156`
- Modify: `src/rquant/midday_briefing.py:597-688`
- Modify: `src/rquant/midday_briefing.py:896-935`

**Step 1: 写失败的排名和手机截断渲染测试**

新增测试构造当前与上一槽的 KPL 成员/快照，覆盖四种 Top5 名次变化：上一名次 3→1 为
`↑2`、1→3 为 `↓2`、2→2 为“持平”、上一 Top5 没有的当前题材为“新晋”。断言 10:00
(`has_prev=False`) 的题材行没有 `↑`、`↓`、“持平”或“新晋”。

再构造三只同为最高 3 板的标的，断言 `render_pulse()` 输出：

```markdown
## 题材 Top5
- 题材A：涨停3 ｜ 最高3板：甲、乙等3只 ｜ ↑2
```

并断言不输出第三只名称；一只或两只同高时，分别完整显示一只或两只名称。

**Step 2: 运行测试，确认当前代码失败**

Run: `pytest -q tests/unit/test_midday_briefing.py -k 'pulse_theme_rank_change or pulse_theme_highest_board_mobile_limit or pulse_first_slot_hides_theme_rank_change'`

Expected: FAIL，因为当前 `ThemeHeat` 没有最高板或排名字段，正文仍为“题材热度”。

**Step 3: 将脉搏计算改接统一梯队摘要**

将 `compute_pulse_view()` 的 `theme_map` 参数替换为 `kpl_members`，再接收本槽可复用的
`prev_limit`。当前槽和上一槽分别调用同一个题材摘要计算；用两份已经 `head(5)` 的结果建立
上一 Top5 的 `{theme: one_based_rank}`。当前 Top5 若不在该映射中标“新晋”，否则计算
`previous_rank - current_rank`。若 `has_prev` 为假或上一槽 boards/snapshot 文件缺失，保留
排名变化为隐藏状态。

在 `render_pulse()` 把“题材热度”节改为“题材 Top5”。新增一个私有格式化 helper，从最高
非空 rung 取名称，最多显示两个；第三只及以后格式为“前两只等N只”。rung 不存在时不渲染
该项（防御性路径）。市场温度、新晋涨停、放量异动、标题、降级短讯和共享数据源注脚不变。

**Step 4: 运行渲染及既有脉搏测试，确认变绿**

Run: `pytest -q tests/unit/test_midday_briefing.py -k 'pulse or U3Delta or U7Rendering'`

Expected: PASS。

**Step 5: 提交脉搏格式检查点**

```bash
git add src/rquant/midday_briefing.py tests/unit/test_midday_briefing.py
git commit -m "feat(pulse): show ranked theme leaders"
```

### Task 3: 让午间复用梯队并改候选池为逐票三行

**Files:**
- Modify: `tests/unit/test_midday_briefing.py`
- Modify: `src/rquant/midday_briefing.py:191-205`
- Modify: `src/rquant/midday_briefing.py:937-1022`
- Modify: `src/rquant/midday_briefing.py:1140-1195`

**Step 1: 写失败的午间统一渲染测试**

将当前 `test_digest_five_sections` 的 fixture 改为一条 `theme_ladders`。断言午间只有
`## ② 题材连续梯队 Top5`，不再有独立的“连板梯队”或“最强题材”标题，并且保留：

```markdown
1. 人形机器人 涨停3 半日额1.2亿（上午 1/2/3/3）
   3板：样本01
   2板：0
   首板：样本02
```

再新增两个候选项，断言每票恰为名称代码、题材+量比、涨幅+距涨停三行，两个区块之间有
空行；断言正文不含表头 `| 代码 |` 或表格分隔线 `|---|`。

**Step 2: 运行测试，确认当前代码失败**

Run: `pytest -q tests/unit/test_midday_briefing.py::TestU7Rendering::test_digest_five_sections`

Expected: FAIL，因为现有 digest 仍渲染两节主题内容和 Markdown 表格。

**Step 3: 最小改造 DigestView、组装和渲染**

用 `theme_ladders: list[ThemeLadderSummary]` 替代 `ladder` 与 `top_themes`。在
`_build_digest_view()` 从当前快照、T-1 涨停榜、KPL 成员和已落盘的上午 snapshot frames
计算一次摘要并传入 view；保留情绪温度、昨终值、持仓体检和现有候选筛选函数。

在 `render_digest()` 用单一“② 题材连续梯队 Top5”节逐题材输出涨停数、半日额和四槽走势，
再逐 rung 输出 `首板` 或 `N板`；空 `stocks` 明确输出 `0`。候选区移除全部表格行，采用：

```python
lines.extend([
    f"- {c.name}（{c.ts_code}）",
    f"  题材：{c.theme or '—'} ｜ 半日量比：{c.vol_ratio:.2f}",
    f"  涨幅：{c.pct_chg:+.1f}% ｜ 距涨停：{c.room_to_limit_pct:.1f}%",
    "",
])
```

最后保留现有 `rstrip()`，避免报告文件末尾无谓空行。

**Step 4: 运行午间及回归测试，确认变绿**

Run: `pytest -q tests/unit/test_midday_briefing.py -k 'digest or candidate or BoardLadder'`

Expected: PASS。

**Step 5: 提交午间格式检查点**

```bash
git add src/rquant/midday_briefing.py tests/unit/test_midday_briefing.py
git commit -m "feat(midday): format theme ladders for mobile"
```

### Task 4: 只读连接复用与 fail-soft 编排

**Files:**
- Modify: `tests/unit/test_midday_briefing.py`
- Modify: `src/rquant/midday_briefing.py:1032-1089`
- Modify: `src/rquant/midday_briefing.py:1140-1210`

**Step 1: 写失败的脉搏单连接测试**

以非 fake 路径 mock `open_readonly_store` 返回一个可关闭的 store，并 mock
`fetch_slot_frames` 返回离线快照。断言一次 `run_morning_pulse()`：

```python
open_readonly_store.assert_called_once()
assert load_kpl_concept_members.call_args.args == (store,)
assert load_prev_limit_list.call_args.args == (store,)
assert load_avg_amount_20d.call_args.args == (store,)
store.close.assert_called_once()
```

再让 `open_readonly_store` 抛 `duckdb.IOException`（或普通 `Exception` 的同等 fail-soft
fixture），断言返回码仍为 0、通知仍发出；主题/放量依赖数据为空而不是异常。增加断言确保
生产路径没有引入 `DuckDBStore()` 默认写连接或 `duckdb.connect()`。

**Step 2: 运行测试，确认当前代码失败**

Run: `pytest -q tests/unit/test_midday_briefing.py -k 'morning_pulse_single_readonly_store or morning_pulse_readonly_failure_is_soft'`

Expected: FAIL，因为当前 `run_morning_pulse()` 对 KPL 和均额分别调用无 store 版本，且未取
T-1 涨停榜。

**Step 3: 用一次 readonly store 提供脉搏所需依赖**

在 `run_morning_pulse()` / `run_midday_report()` 网络获取快照前调用既有 `_open_ro_store()`
一次；在同一个短生命周期 `try/finally` 中依次调用
`load_kpl_concept_members(store)`、`load_prev_limit_list(store)`、`load_avg_amount_20d(store)`
（午间再取持仓），随后立即关闭 store，不能跨网络取数持锁。把原始 KPL 成员传给
`fetch_slot_frames(kpl_members=...)` 及相应 view，避免内部二次读取；非 fake 的 store 为
`None` 时用空 DataFrame 且不可回退到无参数 loader。fake 模式例外：显式以 `None` 调用
各 fake-aware loader，以保留离线 fixture。任一 loader 异常均独立降级为空表。

`_build_digest_view()` 只消费编排层已读取的原始成员、昨日涨停榜、均额与持仓，并将成员表
传给统一摘要计算。所有 DB 访问继续只经 `open_readonly_store()` / `_open_ro_store()`；不新增
直连主库、写操作或连接重试循环。

**Step 4: 运行连接、编排和完整模块测试，确认变绿**

Run: `pytest -q tests/unit/test_midday_briefing.py`

Expected: PASS。

**Step 5: 提交编排检查点**

```bash
git add src/rquant/midday_briefing.py tests/unit/test_midday_briefing.py
git commit -m "fix(pulse): reuse readonly store for theme summaries"
```

### Task 5: 版本、变更记录与完整验证

**Files:**
- Modify: `pyproject.toml:3`
- Modify: `src/rquant/__init__.py:3`
- Modify: `CHANGELOG.md:5-20`

**Step 1: 写版本/变更记录前的失败检查**

先检查两个运行时版本仍未对齐且尚未标记该功能：

Run: `rg -n 'version =|__version__|题材连续梯队|候选观察池' pyproject.toml src/rquant/__init__.py CHANGELOG.md`

Expected: `pyproject.toml` 为 `0.28.1`、`src/rquant/__init__.py` 仍为旧值，且
`CHANGELOG.md` 的 `[Unreleased]` 没有此次题材梯队/脉搏格式条目。

**Step 2: 写最小版本与 changelog 更新**

将 `pyproject.toml` 和 `src/rquant/__init__.py` 都改为 `0.28.2`。在 `[Unreleased]` 的
`Changed`（无则新增）记录：KPL 多对多题材连续梯队、脉搏 Top5 的最高板与排名变化、
午间候选逐票三行格式，以及只读 store 的复用/fail-soft 边界；不要声称修改了调度或通知
直通配置。

**Step 3: 运行静态与完整测试验证**

Run: `ruff check src/rquant/midday_briefing.py tests/unit/test_midday_briefing.py`

Expected: PASS。

Run: `pytest -q tests/unit/test_midday_briefing.py`

Expected: PASS。

Run: `pytest -q`

Expected: PASS（网络标记测试按项目默认配置跳过）。

**Step 4: 检查改动范围与版本一致性**

Run: `git diff --check && git diff -- src/rquant/midday_briefing.py tests/unit/test_midday_briefing.py pyproject.toml src/rquant/__init__.py CHANGELOG.md`

Expected: 无空白错误；改动只覆盖题材梯队、脉搏/午间渲染、单连接只读编排、测试与版本记录。

**Step 5: 提交发布候选**

```bash
git add src/rquant/midday_briefing.py tests/unit/test_midday_briefing.py pyproject.toml src/rquant/__init__.py CHANGELOG.md
git commit -m "feat(midday): improve theme ladder pulse formatting"
```

随后创建 PR，待 Python 3.11/3.12 CI 全绿并 squash merge 后，按受控发布流程在合并后的
`origin/main` 创建 annotated tag `v0.28.2`。日常生产发布仅可执行：

```bash
bash scripts/deploy-production.sh --target v0.28.2
```

本计划不修改 systemd、CLI 或 notify 直通；发布后在允许窗口内用 `rquant morning-pulse
--slot 10:00 --dry-run` 和 `rquant midday-report --dry-run` 做一次人工 PushDeer 正文核对。
