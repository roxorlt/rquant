# rQuant 可信研究平台与策略闭环 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 在不扩大到实盘下单、Tick/Level2 和高频交易的前提下，把 rQuant 从“有数据、有回放、有告警”升级为可证明无未来函数、回放与盘中一致、账户收益可对账、参数选择有严格样本外约束、策略能够进入前瞻模拟盘的可信研究平台。

**Architecture:** 保留生产 DuckDB、只读副本、研究 DuckDB 和分区 Parquet 数据湖的现有职责；先按策略独立关闭 Stage 1 数据与执行快照，再建立统一 `FeatureContext -> StrategySpec -> StateMachine -> Execution/Portfolio` 内核。历史 replay 与 live 使用同一策略规格和特征语义，计算实现允许批量/增量不同；Strategy Lab 只提交 typed run spec 和读取后台任务，不在 Streamlit rerun 中执行长计算。

**Tech Stack:** Python 3.11/3.12、Pydantic、pandas/numpy、DuckDB、Parquet、SQLite WAL、Tushare、Streamlit、systemd、pytest、ruff；本计划首轮不新增机器学习框架、消息队列、PostgreSQL、容器或新付费数据权限。

---

## 0. 执行原则

### 0.1 取代关系

本计划从 2026-07-22 起作为以下两份计划的执行续篇，不删除旧文档：

- `docs/plans/2026-07-13-rquant-trustworthy-strategy-roadmap.md`
- `docs/plans/2026-07-16-research-cloud-alert-lab-implementation.md`

旧计划提供设计依据；本文件以生产 `v0.25.4`、云端研究湖和 2026-07-22 真实审计状态为起点，决定后续顺序。

### 0.2 当前事实基线

| 项目 | 2026-07-22 事实 | 本计划处置 |
|---|---|---|
| 生产版本 | `v0.25.4` / `0909fa3135c6f6ce42c9ced05040e1c47f6cc730` | 作为后续发布基线 |
| N 字 Stage 1 | 917 个资格样本；B、S、历史基准覆盖均为 100%；manifest 已完成 | 独立生成 snapshot/binding 并正式回放 |
| 科创/创业放量 Stage 1 | 5,493/5,493 任务完成；写入 37,252,334 行分钟数据 | 独立生成 snapshot/binding，按修正内外盘方向重跑 |
| 集合竞价 Stage 1 | 21,726 个任务仅完成 627，剩余 21,099；静态需求约 1.47 亿行/14GB | 停止独立 B 策略全量回补，保留已下载证据 |
| 研究权威 | `degraded`；`watchlist_code_commit_mismatch`；稳定日 0 | 用显式采集语义指纹替代“整仓 SHA 必须相等” |
| 模拟盘 | `paper_position`、`paper_position_event`、`intraday_feature_snapshot` 均为 0 行 | 账户回放通过后再开启 shadow paper |
| Strategy Lab | `strategy_lab.py` 约 2,724 行，仍使用顶层 `st.tabs` | 路由、任务、结果模型分层 |
| 策略状态 | N 字、放量、竞价独立 B 和 surge-watch 均未达到 `paper_candidate` | 不宣传收益，不自动升级 |

### 0.3 强制阶段门

1. 一次只推进一个阶段；阶段门未通过，不开始下一阶段业务实现。
2. 所有行为改动先写失败测试；所有生产发布走 PR、CI、精确 tag/SHA 和受控部署器。
3. 工作日 `09:15-15:10` 不修改生产数据、不部署、不重启；分钟监控只做只读验收。
4. systemd 变更先在云端原样运行 `systemd-analyze verify/calendar`，通过后才提交。
5. 生产数据库修复、systemd/nginx/sudoers 安装仍需在对应阶段取得单独明确授权。
6. 新参数只在内层训练/验证区选择；外层测试和前瞻区间不得参与选择。
7. 参数冻结后的 live 样本不得因“结果不好”被删除、改标签或与新版本混算。
8. 研究目标是减少自我欺骗，不承诺稳定收益；任何收益必须同时展示样本、费用、回撤和置信区间。

### 0.4 版本与发布批次

| 版本方向 | 内容 | 主要阶段门 |
|---|---|---|
| `v0.26.x` | Stage 1 解耦收口、manifest 终止语义、研究采集指纹 | N 字/放量独立 comparable；研究增量不被无关提交误降级 |
| `v0.27.x` | PIT 分钟特征引擎 | 前缀不变与 replay/live 特征一致 |
| `v0.28.x` | StrategySpec、状态机、执行与账户回放 | 信号、成交、现金、持仓逐笔可对账 |
| `v0.29.x` | 嵌套 walk-forward、统计惩罚、现有策略重评 | 外层测试不参与选择；形成保留/淘汰结论 |
| `v0.30.x` | 前瞻模拟盘与每日对账 | 冻结版本连续运行 20-40 个交易日 |
| `v0.31.x` | Strategy Lab 收口与下一个新策略 | 新策略可通过统一模板接入，不复制回放框架 |

---

## 1. Stage A：Stage 1 解耦与资源止损

### Task A1：为回补 manifest 增加可审计终止状态

**Files:**
- Modify: `src/rquant/backfill_state.py`
- Modify: `src/rquant/intraday_backfill.py`
- Modify: `src/rquant/cli.py`
- Test: `tests/unit/test_backfill_state.py`
- Test: `tests/unit/test_backfill_cli.py`
- Modify: `CHANGELOG.md`

**Step 1: 写失败测试**

增加以下行为测试：

```python
def test_abandon_manifest_is_terminal_and_preserves_completed_tasks() -> None:
    # running manifest: succeeded=627, pending=21099
    # abandon 后 terminal=True、status=abandoned；已完成任务与统计保持不变。
    ...

def test_abandon_manifest_requires_expected_identity_and_reason() -> None:
    # manifest id、当前状态或 plan hash 不一致时 fail closed。
    ...
```

**Step 2: 运行红灯**

Run:

```bash
.venv/bin/python -m pytest -q \
  tests/unit/test_backfill_state.py \
  tests/unit/test_backfill_cli.py
```

Expected: 因不存在 `abandoned` 状态和 CLI 而失败。

**Step 3: 最小实现**

1. 在 SQLite manifest 状态机增加 `abandoned` 终态，不删除 task、attempt、coverage 或已写分钟数据。
2. 新增 `rquant backfill-abandon --manifest-id ... --reason ...`：默认 dry-run；`--apply` 才在短事务内 CAS 更新。
3. abandon 后 `backfill-run` 必须拒绝领取任务；`backfill-status` 显示终止原因、时间、操作者代码 commit、剩余任务数。
4. 不提供“恢复 abandoned”快捷入口；要继续必须生成新 manifest，避免旧身份被改写。

**Step 4: 运行绿灯与全量相关测试**

Run:

```bash
.venv/bin/python -m pytest -q tests/unit/test_backfill_*.py
.venv/bin/python -m ruff check src/rquant/backfill_state.py \
  src/rquant/intraday_backfill.py src/rquant/cli.py tests/unit/test_backfill_*.py
```

Expected: 全部通过。

**Step 5: 提交**

```bash
git add src/rquant/backfill_state.py src/rquant/intraday_backfill.py \
  src/rquant/cli.py tests/unit/test_backfill_state.py \
  tests/unit/test_backfill_cli.py CHANGELOG.md
git commit -m "feat(research): add auditable backfill abandonment"
```

### Task A2：把 Stage 1 验收改为按策略独立运行

**Files:**
- Create: `src/rquant/stage1_acceptance.py`
- Create: `scripts/run-stage1-strategy-acceptance.sh`
- Modify: `src/rquant/cli.py`
- Test: `tests/unit/test_stage1_acceptance.py`
- Test: `tests/unit/test_stage1_strategy_acceptance_script.py`
- Modify: `docs/deploy/2026-07-22-stage1-strategy-closeout.md`

**Step 1: 写失败测试**

覆盖：

- `--strategy n_shape` 不读取或等待 growth/auction manifest。
- `--strategy growth_board_surge` 只验证自己的 completed manifest、repair、snapshot 和 formal smoke。
- `auction_gap` manifest 为 abandoned 时不会阻塞其他策略。
- 任一阶段失败都保留证据、恢复原 timers，并停在首个失败点。
- 同一策略重复执行复用已完成 snapshot/binding，不生成冲突记录。

**Step 2: 实现 typed acceptance spec**

新增 `Stage1AcceptanceSpec`，固定：

```python
class Stage1AcceptanceSpec(BaseModel):
    strategy: Literal["n_shape", "growth_board_surge", "auction_gap"]
    manifest_id: str
    start_date: date
    end_date: date
    expected_code_commit: str
```

流程严格为：read-only status -> minute repair preview/apply -> snapshot preview/apply -> data audit -> formal smoke -> replica sync -> preflight。

**Step 3: 增加 dry-run 与耗时预算**

运行前输出：是否需 repair、预计 snapshot 扫描量、正式回放样本上限、下一交易保护窗口、所需磁盘临时空间。dry-run 不写主库、不写研究 catalog。

**Step 4: 测试**

```bash
.venv/bin/python -m pytest -q \
  tests/unit/test_stage1_acceptance.py \
  tests/unit/test_stage1_strategy_acceptance_script.py \
  tests/unit/test_formal_smoke_replay.py \
  tests/unit/test_research_snapshot.py
```

**Step 5: 提交**

```bash
git add src/rquant/stage1_acceptance.py scripts/run-stage1-strategy-acceptance.sh \
  src/rquant/cli.py tests/unit/test_stage1_acceptance.py \
  tests/unit/test_stage1_strategy_acceptance_script.py \
  docs/deploy/2026-07-22-stage1-strategy-closeout.md
git commit -m "feat(research): decouple stage1 strategy acceptance"
```

### Task A3：生产研究操作与验收

**No code changes. Production/research operation.**

**Step 1:** 在盘外对集合竞价 manifest 执行 abandon dry-run，核对：

- manifest 精确为 `3d5893dddfa0f8cd17cddec40701c216e423e9d818697dfff9a5d71b60200d3c`；
- succeeded=627、pending=21099、failed=0；
- 不删除 4,267,387 已写行；
- 只改变研究任务状态 SQLite，不改生产 DuckDB。

**Step 2:** 获得生产研究状态写入授权后 apply；保留 JSON 审计文件。

**Step 3:** 分别 dry-run/apply N 字和科创/创业 acceptance；每个策略独立取得：

- `audit_run_id`
- `dataset_snapshot_id`
- `dataset_binding_hash`
- `strategy_spec_hash`
- `result_hash`
- sample count、候选数、成交数和初始收益指标

**Step 4:** 验证主副 catalog、lake 文件 hash、生产只读副本、备份与 preflight。

**Stage A exit:** N 字与科创/创业均有 `comparable` 固定回放；集合竞价独立 B 正式标记 `retire`，其竞价数据仍可作为其他策略特征使用。

---

## 2. Stage B：研究采集语义指纹与连续观察

### Task B1：建立 watchlist 采集语义指纹

**Files:**
- Modify: `src/rquant/research_ingest.py:120-310`
- Modify: `src/rquant/research_ingest.py:520-585`
- Modify: `src/rquant/research_ingest.py:2240-2270`
- Modify: `src/rquant/monitor.py`
- Modify: `src/rquant/config.py`
- Test: `tests/unit/test_research_ingest.py`
- Test: `tests/unit/test_monitor.py`

**Step 1: 写失败测试**

```python
def test_commit_change_with_same_watchlist_semantics_remains_candidate() -> None:
    # 盘前/盘后 commit 不同，但 semantics fingerprint、config 和 items 相同。
    ...

def test_semantics_change_fails_closed_even_when_items_happen_to_match() -> None:
    # 规则版本变化必须 degraded，不能只比较当日碰巧相同的股票列表。
    ...

def test_legacy_watchlist_without_semantics_fingerprint_fails_closed() -> None:
    ...
```

**Step 2: 定义版本化模型**

```python
class WatchlistSemantics(BaseModel):
    contract_version: Literal["watchlist-v2"]
    universe_version: str
    pool_rule_version: str
    normalization_version: str
    config_hash: str

    @computed_field
    def fingerprint(self) -> str: ...
```

原 `code_commit` 继续保存用于溯源，但日终兼容性判断改为 `fingerprint`。影响候选语义的代码 PR 必须显式升级对应 version；无关页面、文档、部署或其他策略提交不会中断 observation。

**Step 3: 增加发布纪律检查**

测试 fixture 固定 fingerprint；修改 watchlist 关键函数却未更新 semantics version 时，核心契约测试失败。不要使用运行时 `inspect.getsource()` 生成不稳定哈希。

**Step 4: 验证**

```bash
.venv/bin/python -m pytest -q \
  tests/unit/test_research_ingest.py tests/unit/test_monitor.py
.venv/bin/python -m ruff check src/rquant/research_ingest.py \
  src/rquant/monitor.py tests/unit/test_research_ingest.py tests/unit/test_monitor.py
```

**Step 5: 提交**

```bash
git add src/rquant/research_ingest.py src/rquant/monitor.py src/rquant/config.py \
  tests/unit/test_research_ingest.py tests/unit/test_monitor.py
git commit -m "fix(research): scope watchlist compatibility to semantics"
```

### Task B2：增加研究 authority 观察解释与恢复规则

**Files:**
- Modify: `src/rquant/research_ingest.py`
- Modify: `src/rquant/cli.py`
- Create: `src/rquant/research_observation.py`
- Test: `tests/unit/test_research_ingest.py`
- Create: `tests/unit/test_research_observation.py`
- Modify: `src/rquant/dashboard/strategy_lab_data.py`

**Steps:**

1. 先写测试，证明一次 degraded 不会被静默覆盖，后续 candidate 以新连续区间从 1 开始。
2. observation 输出 `compatibility_basis`、盘前/盘后 commit、语义指纹、首个失败点和是否可自动恢复。
3. 新增只读 `research-observation-status --date ...`，回答“为什么被降级、从哪天重新累计、还差几天”。
4. Strategy Lab 数据健康页显示连续区间，不只显示一个 `status`。
5. 不回填或伪造 2026-07-21；它继续作为 `watchlist_code_commit_mismatch` 历史证据。
6. 测试、ruff、提交：`feat(research): explain authority observation continuity`。

**Stage B exit:** 无关 commit 不再重置稳定日；真实规则变化仍 fail closed；连续 10 个交易日晋级规则可解释、可复核。

---

## 3. Stage C：无未来函数分钟特征引擎

### Task C1：定义 FeatureContext 与 FeatureSpec

**Files:**
- Create: `src/rquant/intraday_features.py`
- Create: `src/rquant/feature_baseline.py`
- Create: `tests/unit/test_intraday_features.py`
- Create: `tests/unit/test_feature_no_lookahead.py`
- Modify: `src/rquant/strategy_dependencies.py`

**Step 1: 写模型与可见性失败测试**

```python
class FeatureContext(BaseModel):
    trade_date: date
    as_of_time: datetime
    visible_minute_end: datetime
    previous_trade_date: date
    price_basis: Literal["raw", "qfq_pit"]
    dataset_snapshot_id: str
    mode: Literal["replay", "live"]
```

测试盘后字段在可见时间前访问会得到 typed unavailable，而不是零或 NaN 静默继续。

**Step 2: 实现首批共同特征**

- 同分钟成交额比：当前 `HH:MM` / 过去 N 日同一 `HH:MM` 中位数；
- 累计成交进度：截至当前分钟 / 过去 N 日同刻累计中位数；
- 1/5/10 分钟成交额加速度；
- VWAP、距 VWAP、VWAP 斜率、跌破后收复；
- 20/60/120 日价格百分位；
- 距涨停、前高、ATR/历史波动率；
- 90 日筹码峰距离、下方支撑密度、上方套牢密度、分布集中度和峰迁移；
- 竞价强度与市场/板块上下文的 PIT 包装。

所有输出为连续值 + `available` + `reason`，不在特征层直接硬过滤。

**Step 3: 前缀不变测试**

对每个分钟 `t`：

1. 用 `[start, t]` 计算一次；
2. 追加 `[t+1, close]` 后重算；
3. 断言 `t` 及以前全部特征逐值一致。

**Step 4: 开盘段参数化测试**

`09:30`、`09:30-09:31`、`09:30-09:32` 和 `09:30-09:34` 是可比较 segment，不预设前三分钟必然单独或最优。09:30 只能和历史 09:30 比，不能进入普通 5 分钟滚动分母。

**Step 5: 运行与提交**

```bash
.venv/bin/python -m pytest -q tests/unit/test_intraday_features.py \
  tests/unit/test_feature_no_lookahead.py
.venv/bin/python -m ruff check src/rquant/intraday_features.py \
  src/rquant/feature_baseline.py tests/unit/test_intraday_features.py \
  tests/unit/test_feature_no_lookahead.py
git commit -m "feat(strategy): add point-in-time intraday features"
```

### Task C2：建立 replay/live 特征一致性与性能预算

**Files:**
- Create: `tests/integration/test_feature_replay_live_parity.py`
- Create: `src/rquant/feature_runtime.py`
- Modify: `src/rquant/monitor.py`
- Modify: `src/rquant/surge_watch.py`
- Modify: `src/rquant/minute_replay.py`

**Steps:**

1. 固定同一份 241 分钟 fixture，历史批量和 live 增量逐分钟输出完全一致。
2. 把 v0.25.4 的跨日、同分钟重复、乱序、午休和重启 seed 纳入统一 fixture。
3. 增加 stage timing：API、normalize、baseline、features、strategy、persist、notify 分段记录 p50/p95。
4. 目标池 300 只时单轮 p95 < 10 秒；创业/科创约 2,000 只粗特征单轮 p95 < 30 秒；超预算先做向量化和 baseline 预计算，不先升级服务器。
5. 生产接入先以 shadow 方式只落摘要，不改变现有告警；对照至少 3 个交易日后再切换。
6. 测试、提交：`feat(strategy): align replay and live feature runtime`。

**Stage C exit:** 所有共同特征通过前缀不变、PIT 可见性、复权和 replay/live 一致性测试；性能留有一分钟预算。

---

## 4. Stage D：统一 StrategySpec 与信号状态机

### Task D1：定义策略规格和事件模型

**Files:**
- Create: `src/rquant/strategy_spec.py`
- Create: `src/rquant/strategy_engine.py`
- Create: `src/rquant/strategies/__init__.py`
- Create: `tests/unit/test_strategy_spec.py`
- Create: `tests/unit/test_strategy_engine.py`

**Models:**

```python
class StrategySpec(BaseModel):
    strategy_name: str
    strategy_version: str
    universe: UniverseSpec
    entry: EntrySpec
    ranking: RankingSpec
    exit: ExitSpec
    execution: ExecutionSpec
    feature_versions: dict[str, str]

class SignalEvent(BaseModel):
    state: Literal["candidate", "armed", "confirmed", "rejected", "expired"]
    event_time: datetime
    earliest_fill_time: datetime | None
    reasons: tuple[str, ...]
    feature_snapshot_hash: str
```

**Steps:**

1. 写 canonical JSON/hash、未知字段拒绝、版本不可缺失测试。
2. 写 `t` 分钟确认后最早 `t+1` 成交测试。
3. 实现 deterministic state transitions；同一事件重放幂等。
4. 缺特征必须 `rejected/unavailable`，不得按零参与评分。
5. 提交：`feat(strategy): add versioned strategy state engine`。

### Task D2：迁移三个现有策略为 adapter

**Files:**
- Create: `src/rquant/strategies/n_shape.py`
- Create: `src/rquant/strategies/growth_board_surge.py`
- Create: `src/rquant/strategies/auction_context.py`
- Modify: `src/rquant/minute_replay.py`
- Modify: `src/rquant/growth_board_surge_strategy.py`
- Modify: `src/rquant/auction_gap_strategy.py`
- Modify: `src/rquant/monitor.py`
- Modify: `src/rquant/surge_watch.py`
- Create: `tests/integration/test_strategy_replay_live_parity.py`

**Steps:**

1. N 字明确版本：`first_break`、`strong_carry_and_break`、`break_retest`，不能把两条独立 live 提醒当成一个历史 B。
2. 科创/创业放量把 v4 累计比、红盘、1 分钟上涨、近似外盘占优写入同一个 spec；Lab 和 surge-watch 若规则不同，使用不同策略名/version。
3. 竞价不再提供独立直接 B adapter，只产出 `AuctionContextFeature`。
4. 固定 fixture 证明旧 adapter 与新 engine 在兼容版本上逐笔一致；差异必须形成迁移报告。
5. live 先 shadow 双算，不双发 Push；差异率为零或逐项解释后才切换。
6. 提交：`refactor(strategy): unify replay and live strategy semantics`。

**Stage D exit:** 同一 snapshot、spec 和逐分钟前缀在 replay/live 产生相同信号时间、状态、理由和特征 hash。

---

## 5. Stage E：A 股执行模型与 10 万账户回放

### Task E1：实现可成交性引擎

**Files:**
- Create: `src/rquant/execution.py`
- Create: `tests/unit/test_execution.py`
- Modify: `src/rquant/price_adjustment.py`
- Modify: `src/rquant/paper.py`

**Tests first:**

- 信号分钟不能成交，下一分钟才可尝试；
- 一字涨停、封死涨停拒绝买入；一字跌停、封死跌停拒绝卖出；
- T+1 当日止损只记录风险，不产生卖单；
- 100 股整数手、余额不足、最低佣金、印花税和滑点；
- 分钟成交额容量上限导致 partial/rejected；
- 停牌、缺分钟、午休、除权日和无涨跌幅限制日。

**Implementation:**

`ExecutionDecision` 必须返回 `filled/partial/rejected`、数量、价格、费用、原因和引用分钟键。原始价格用于成交与涨跌停判断，复权价格只用于跨日收益和结构比较。

**Commit:** `feat(replay): add A-share execution model`。

### Task E2：实现账户级组合回放

**Files:**
- Create: `src/rquant/portfolio_replay.py`
- Create: `tests/unit/test_portfolio_replay.py`
- Create: `tests/integration/test_account_replay.py`
- Modify: `src/rquant/strategy_compare.py`
- Modify: `src/rquant/paper.py`

**Steps:**

1. 先写 10 万现金、同日多信号 topN、持仓重叠、卖出回款和现金守恒测试。
2. 实现最大持仓数、单票上限、未成交资金递补下一候选和容量约束。
3. 实现 T+1 后结构止损、灾难止损、分批止盈、移动保护和 1-10 日时间退出。
4. 输出逐笔 ledger：订单意图、成交、现金、股数、成本、已实现/未实现盈亏和权益。
5. 加入 invariant：`cash + market_value = equity`，费用逐笔可加总，任何时点不允许负现金/负股数。
6. 提交：`feat(replay): add auditable portfolio account simulation`。

### Task E3：统一结果指标

**Files:**
- Create: `src/rquant/strategy_result.py`
- Create: `tests/unit/test_strategy_result.py`
- Modify: `src/rquant/dashboard/strategy_lab_runs.py`

**Metrics:**

- 资格、触发、确认、成交、拒单、部分成交；
- 净平均/中位收益、胜率、盈亏比、Profit Factor；
- 账户最大回撤、Calmar、资金利用率、换手、费用；
- MAE/MFE、持有期、连续亏损；
- 单票、单日、单月收益贡献集中度；
- 数据覆盖、价格口径、成本模型和容量假设。

**Stage E exit:** 10 万本金估算只能从账户净值生成，不再用平均单笔收益线性年化。

---

## 6. Stage F：严格优化与模型验证

### Task F1：实现嵌套 walk-forward 与 purge/embargo

**Files:**
- Create: `src/rquant/model_validation.py`
- Create: `tests/unit/test_nested_walk_forward.py`
- Modify: `src/rquant/topn_walk_forward.py`
- Modify: `src/rquant/strategy_optimizer.py`

**Steps:**

1. 构造“外层测试特别好、内层普通”的候选，证明它不会因测试好被选中。
2. 外层按时间多折；每个外层训练区间内部再切训练/验证选择参数。
3. purge 长度至少为最大持有期，embargo 防止相邻折共享退出标签。
4. 外层测试只评分一次，不能写入 `robust_score` 或候选排序。
5. 保存全部尝试组合数、随机种子、snapshot、spec hash 和每折日期。
6. 提交：`fix(research): isolate outer walk-forward evaluation`。

### Task F2：增加多重试验与不确定性报告

**Files:**
- Modify: `src/rquant/model_validation.py`
- Create: `tests/unit/test_model_validation.py`
- Modify: `src/rquant/strategy_optimizer.py`

**Steps:**

1. 用 block bootstrap 计算净期望和最大回撤区间，保留时间相关性。
2. 计算 Deflated Sharpe 或等价选择偏差惩罚；小样本返回 unavailable，不伪造数字。
3. 增加 PBO/参数排名翻转诊断；组合不足时明确说明不可计算。
4. 输出参数邻域稳定性，孤立最优点自动降级。
5. 目标函数为样本外净收益减回撤、换手、集中度和小样本惩罚；不允许只最大化胜率。
6. 提交：`feat(research): quantify backtest selection risk`。

### Task F3：缓存特征与逐级淘汰搜索

**Files:**
- Create: `src/rquant/feature_matrix_cache.py`
- Create: `tests/unit/test_feature_matrix_cache.py`
- Modify: `src/rquant/feature_weight_search.py`
- Modify: `src/rquant/risk_search.py`
- Modify: `src/rquant/dashboard/strategy_lab_worker.py`

**Steps:**

1. 同一 snapshot + feature versions 只计算一次候选分钟特征矩阵。
2. 搜索先粗网格，再在稳定区域细化；明显劣势候选用 successive halving 提前终止。
3. ETA 由历史同类任务 p75 + 扫描量 + 组合数估算，展示低/中/高置信度。
4. 缓存绑定 snapshot/spec hash，任何源工件变化都拒绝复用。
5. 不在本阶段引入 Optuna、LightGBM 或 sklearn；先证明透明因子和验证链可靠。

**Stage F exit:** 自动优化无法读取外层测试选参；每个“最佳策略”同时显示试验次数、置信区间、PBO/DSR 和参数稳定区。

---

## 7. Stage G：现有策略科学重评

### Task G1：N 字 Pool1+Pool2

**Research questions:**

1. `first_break`、`strong_carry_and_break`、`break_retest` 哪个在外层样本外更稳定？
2. 全量触发与同分钟 topN 排名相比，账户收益和回撤是否改善？
3. 90 日筹码、竞价、同刻累计、加速度、价格位置、市场/板块环境各自提供多少增量？
4. 收益来自当日冲高、次日溢价还是更长持有？
5. 动态 S 是否在扣费、T+1 和不可卖约束后仍优于固定持有？

**Labels:** 1/3/5/10 日 MFE、MAE、收盘收益、首次触板时间、封板持续、最大回撤和真实退出收益。

**Ablations:** 每次只移除一个特征族；保持同一候选、同一成交模型和外层折。少于 100 笔外层成交不晋级。

**Output:** `docs/analysis/YYYY-MM-DD-n-shape-v2-formal-review.md`，结论只能是 `retire`、`observe`、`paper_candidate`。

### Task G2：科创/创业放量与 surge-watch

**Research questions:**

1. 修正后的外盘占优是否对外层收益有正向增量？旧反向实现结果全部失效。
2. 纯累计比、同分钟比、1/5/10 分钟加速度分别贡献什么？
3. 开盘 09:30/2/5 分钟是否需要独立模型，由 walk-forward 选择而非先验写死。
4. `ratio_cap=8`、红盘、1 分钟上涨、近似外盘占优是过滤毒尾还是丢失延续机会？
5. Push 后 1/3/5/10 日 MFE/MAE 与账户 S 结果如何，不只看当日是否涨停。

**Live evidence:** `v0.25.4` 之后先积累至少 10 个无数据故障交易日才重新校准；单日和事故日不得参与阈值选择。

**Output:** Lab 回放策略与 surge-watch live 若不能统一，明确命名为两个版本并分别评价。

### Task G3：集合竞价因子迁移

不再研究“09:27 筛选后直接开盘 B”。只验证竞价因子是否提升：

- N 字候选排序；
- 放量策略真假强势过滤；
- 首板次日弱转强候选。

只回补这些候选的竞价和分钟窗口。若多个外层折均无增益，保留原始数据和研究记录，删除 live 交易入口。

**Stage G exit:** 两个现有主策略得到正式保留/淘汰决定；没有策略因“最高平均收益好看”直接进入生产提醒。

---

## 8. Stage H：前瞻模拟盘与每日对账

### Task H1：shadow paper 生产闭环

**Files:**
- Modify: `src/rquant/monitor.py`
- Modify: `src/rquant/paper.py`
- Create: `src/rquant/paper_review.py`
- Modify: `src/rquant/storage/schema.py`
- Modify: `src/rquant/storage/migrations.py`
- Create: `tests/integration/test_live_paper_cycle.py`
- Test: `tests/unit/test_paper.py`
- Modify: `src/rquant/cli.py`

**Steps:**

1. 先写 candidate -> signal -> next-minute fill -> T+1 hold -> partial exit -> close 全周期测试。
2. 只有 `paper_candidate` 的冻结 spec 可进入生产 shadow；每个版本独立持仓和绩效。
3. `rquant-monitor` 仍是盘中唯一 DuckDB writer，写特征快照、订单意图、持仓和事件。
4. Push 显示策略/version、B/S 原因、模拟成交状态、风险线和剩余仓位；不发送实盘订单。
5. 新 schema migration 可重复、可回滚验证；发布前另行取得生产 DB 写入授权。
6. 提交：`feat(paper): run frozen forward strategy accounts`。

### Task H2：每日 replay/live 对账和漂移监控

**Files:**
- Modify: `src/rquant/paper_review.py`
- Create: `tests/unit/test_paper_review.py`
- Modify: `src/rquant/cli.py`
- Modify: `src/rquant/preflight.py`

**Checks:**

- live 分钟与日终历史分钟差异；
- live/replay 同刻特征和信号 hash；
- 下一分钟模拟成交对应真实可见分钟；
- 现金、股数、费用和 T+1 余额；
- 特征分布漂移、触发率漂移和成交拒绝率；
- 数据源延迟、漏分钟、重启恢复和通知去重。

差异超阈值自动把策略降级为 `observe` 并停止新开仓，不改写既有持仓历史。

### Task H3：前瞻晋级门

1. 参数冻结日起至少 20 个交易日初评，优选 40 日正式判断。
2. 至少 30 笔前瞻成交；不足则延长，不降低门槛。
3. 历史与前瞻的收益、胜率、拒单率和因子分布差异可解释。
4. 最大回撤和连续亏损不超过预先写入 spec 的风险预算。
5. 通过后才可成为 `monitor_approved`；仍只提醒、人决策，不自动下单。

---

## 9. Stage I：Strategy Lab 工作流重构

### Task I1：按需路由与表单提交

**Files:**
- Create: `src/rquant/dashboard/lab/__init__.py`
- Create: `src/rquant/dashboard/lab/router.py`
- Create: `src/rquant/dashboard/lab/context.py`
- Create: `src/rquant/dashboard/lab/pages/data_health.py`
- Create: `src/rquant/dashboard/lab/pages/research.py`
- Create: `src/rquant/dashboard/lab/pages/jobs.py`
- Create: `src/rquant/dashboard/lab/pages/archive.py`
- Create: `src/rquant/dashboard/lab/pages/monitor.py`
- Create: `src/rquant/dashboard/lab/pages/paper.py`
- Modify: `src/rquant/dashboard/strategy_lab.py:1253-2724`
- Create: `tests/unit/test_strategy_lab_router.py`

**Steps:**

1. 测试选择一个页面时，不导入、不查询、不执行其他页面。
2. 用侧栏导航替代顶层 `st.tabs`；每次 rerun 只 render 一个页面。
3. 所有运行参数进入 `st.form`，编辑控件不触发扫描，提交后才生成 `StrategyRunSpec`。
4. 页面只读研究/生产副本，不持有主库 writer。
5. Playwright 验证桌面和手机无重叠，切页面不丢后台任务。
6. 提交：`refactor(lab): route one research workspace at a time`。

### Task I2：持久任务中心

**Files:**
- Create: `src/rquant/lab_jobs.py`
- Create: `src/rquant/dashboard/lab/job_center.py`
- Create: `tests/unit/test_lab_jobs.py`
- Modify: `src/rquant/dashboard/strategy_lab_worker.py`

**Steps:**

1. SQLite WAL 保存 queued/running/succeeded/failed/cancelled、lease、heartbeat 和 spec hash。
2. Streamlit 只提交任务；一个受限 worker 执行长回放。
3. 分片更新 completed/total、EWMA ETA、预计完成时间和当前阶段。
4. 页面关闭后任务继续；支持取消、stale lease 恢复、失败重试和原 spec 重跑。
5. 工作日盘中只允许只读轻任务；大优化在 Mac worker 或独立研究 worker 运行。
6. 提交：`feat(lab): add durable research job center`。

### Task I3：结果驾驶舱、实验对比和 ELI25 说明

**Files:**
- Create: `src/rquant/dashboard/lab/result_overview.py`
- Create: `src/rquant/dashboard/lab/run_compare.py`
- Create: `src/rquant/dashboard/lab/help_content.py`
- Test: `tests/unit/test_strategy_result.py`
- Modify: `src/rquant/dashboard/strategy_lab_runs.py`

**First screen:**

- 这次实验能/不能说明什么；
- 数据覆盖与研究状态；
- 样本外账户收益、回撤和成交数；
- 最主要风险和与基线的唯一变化。

支持勾选 2-5 次 run，按相同股票/日期配对比较；消融说明使用 25 岁非量化用户能理解的中文，专业公式放在展开区。历史记录按策略、版本、状态和日期检索，完整表格按需加载并导出 Markdown。

**Stage I exit:** 页面切换不执行隐藏策略、不丢结果；运行前有 ETA；结果先解释可信度，再展示最优收益。

---

## 10. Stage J：下一策略与平台扩展模板

### Task J1：策略脚手架

**Files:**
- Create: `src/rquant/strategy_template.py`
- Create: `tests/unit/test_strategy_template.py`
- Modify: `docs/strategy-development-guide.md`

输入一份新策略描述后，必须先填写：Universe、Feature、Entry、Ranking、Exit、Execution、数据可见时间、资格分母和停止条件。工具生成 adapter、spec、fixture 和回补需求，不生成未经验证的“最优参数”。

### Task J2：首板质量 -> 次日弱转强

当前策略完成 Stage G 后才启动。使用已有涨停明细、竞价、分钟、日线和板块数据；研究：

- B 日封板时间、开板次数、封单金额、成交结构；
- 次日竞价强度、缺口、换手和板块共振；
- 次日开盘后 1/5/10 分钟承接、VWAP 和同刻放量；
- T+1 后真实 S 和无法卖出风险。

先生成资格全集和最小回补 manifest；未达到覆盖门不回测。完成同样的 nested walk-forward、账户回放和前瞻晋级流程。

### Task J3：后续候选顺序

1. 龙虎榜机构净买入 + 1/3 日延续；
2. 多日资金流积累 + 低位突破；
3. 题材/行业轮动 + 个股共振；
4. 隔夜收益与日内收益分解；
5. ETF 低波/动量轮动。

真实外盘/内盘和大单方向若需要 Tick/Level2，必须重新立项；分钟 OHLCV 的 tick-rule 近似不得标成真实订单流。

---

## 11. 数据、算力和预算

1. 当前 Tushare 历史分钟、实时分钟、实时日累计和竞价权限足够完成 Stage A-I，不购买新权限。
2. 生产 2 vCPU 用于 live、日终增量和轻量回放；不在盘中跑全量自动优化。
3. 先用特征缓存、分区裁剪和逐级淘汰降低复杂度；优化仍经常超过 30 分钟时，再启用 Mac worker 或独立 4-8 vCPU 研究机。
4. 集合竞价未完成 manifest 停止后，可避免继续约 1.47 亿行/14GB 的低价值下载。
5. 云盘达到 60% 或预计 90 天达到 70% 时再启用 COS；原始不可变分区、catalog 和异机备份必须可恢复。

---

## 12. 每阶段通用验证

```bash
.venv/bin/python -m pytest -q
.venv/bin/python -m ruff check src/rquant tests
.venv/bin/rquant preflight
git diff --check
```

额外要求：

- DuckDB：验证 monitor 写主库、页面读副本，没有第二个盘中 writer。
- PIT：运行前缀不变、可见时间和复权测试。
- Strategy：固定同一分钟 replay/live parity。
- Execution：逐笔 ledger 与现金/仓位守恒。
- Optimizer：测试外层结果不能影响选择，记录全部试验数。
- Streamlit：Playwright 桌面/手机截图、无重叠、长任务可恢复。
- systemd：云端 `systemd-analyze verify` 和 5 次 calendar iteration。
- 生产：dry-run -> 精确 tag/SHA 部署 -> preflight -> 备份 -> 副本 -> timers/services -> `DEPLOY.md` PR。

---

## 13. 停止条件与完成定义

### 13.1 单策略停止条件

满足任一条件即停止继续调参，转为 retire/observe：

- 完整覆盖后多个外层折净期望仍为负；
- 正收益只来自单只股票、单日或单月；
- 参数稍微变化即由正转负；
- 扣费、不可成交和账户约束后优势消失；
- 多重试验惩罚后无法区别于随机；
- 前瞻表现持续偏离回放且无法由数据/成交解释。

### 13.2 本计划完成定义

- N 字和科创/创业获得独立、不可变、可复现的 Stage 1 comparable 证据；
- 集合竞价独立 B 已审计式退出，不再阻塞其他策略；
- 研究 authority 连续观察不被无关代码提交重置；
- 共同分钟特征无未来函数，replay/live 同刻一致；
- 三策略通过统一 StrategySpec/状态机运行；
- 10 万账户回放支持 T+1、费用、涨跌停、容量和动态 S；
- 自动优化采用嵌套 walk-forward、purge/embargo 和多重试验惩罚；
- 现有策略得到正式 retire/observe/paper_candidate 结论；
- 至少一个冻结策略运行 20-40 个交易日前瞻模拟并每日对账；
- Strategy Lab 可按工作流运行、估时、恢复、比较和解释实验；
- 新策略可通过模板快速接入，但仍必须经过同样阶段门。

## 14. 第一批立即执行清单

本计划合并后只启动 Stage A，不提前实现 Stage C-J：

1. 实现 backfill manifest `abandoned` 终态和 dry-run/apply CLI。
2. 实现按单策略独立 Stage 1 acceptance runner。
3. PR、CI、发布代码。
4. 盘外 dry-run 核对集合竞价 manifest，取得生产研究状态写入授权后 apply。
5. 分别完成 N 字和科创/创业 snapshot/binding/formal smoke。
6. 更新两份正式分析报告和 `DEPLOY.md`。
7. Stage A 验收通过后再启动 Stage B。
