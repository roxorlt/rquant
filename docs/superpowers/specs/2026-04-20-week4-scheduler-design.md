---
title: Week 4 调度 + N 形态策略 Design
created_at: 2026-04-20
status: draft
owner: roxor
---

# Week 4 — 调度引擎 + N 形态策略

## 1. 背景与动机

Week 3b 完成了筛选规则引擎（18 个积木 + `screen()` 入口）。Week 4 在此基础上做两件事：

1. **数据补全 + N 形态积木**（4a）：接入 `daily_basic`（流通市值等）、暴露 body_upper/body_lower 到宽表、新增 6 个积木支撑完整 N 形态策略
2. **调度框架 + 管道编排**（4b）：APScheduler 常驻进程、CLI 入口、`screen_result` 落库、预设注册表、多池子管道

用户的 N 形态策略是两步筛选 + 后续监控：
- **Pool 1**（T 日收盘后）：昨首板 + 11 条过滤条件 → 候选池
- **Pool 2**（T+1 日收盘后）：Pool 1 子集 → 实体收缩 + 下影线 → 精选池
- **监控**（T+1/T+2 盘中）：Pool 2 标的回踩关键位时告警（Week 5/6 做）

## 2. 范围边界

### Week 4a 做的
- 接入 Tushare `daily_basic` 表（circ_mv / total_mv / turnover_rate）
- `STATE_COLS_MAP` 增加 body_upper / body_lower → 宽表暴露 `BODY_UPPER[n]` / `BODY_LOWER[n]`
- 6 个新积木：has_lower_shadow / circ_mv_lt / not_yiziban / no_consec_ups_in_window / no_limit_down_in_window / has_prior_limit_up
- 长窗口聚合列动态计算（approach B：窗口大小从规则参数传入，load_universe 自动适应）

### Week 4b 做的
- CLI 入口（`rquant serve` + `rquant run-daily`）
- APScheduler BlockingScheduler 常驻进程
- `screen_result` DuckDB 表
- 预设注册表（`presets.py` + `ScreenPreset` 数据模型）
- 多池子管道（Pool 1 → Pool 2 依赖链）
- 每日全流水线编排（拉数据 → 指标 → state → 筛选 → 落库）

### 不做（留给后续）
- ❌ 盘中 tick 级监控（Week 5）
- ❌ 告警推送（Week 6）
- ❌ UI 展示 screen_result（Week 7）
- ❌ APScheduler jobstore 持久化（MVP 用内存）
- ❌ Pool 2 自动触发（4b 实现的 Pool 2 需手动在 T+1 运行 `run-daily`，自动触发等 Week 5 常驻进程成熟后再做）

## 3. N 形态策略规则详解

### 3.1 Pool 1（T 日收盘后运行）

| # | 条件 | 积木 | 状态 |
|---|---|---|---|
| 1 | 非 ST | `not_st()` | ✅ 已有 |
| 2 | 非北交所 | `not_bj()` | ✅ 已有 |
| 3 | T-1 首板 | `first_limit_up(offset=1)` | ✅ 已有 |
| 4 | T 日未涨停 | `not_limit_up(offset=0)` | ✅ 已有 |
| 5 | T-1 涨停不是一字板 | `not_yiziban(offset=1)` | 🆕 4a |
| 6 | T 日最高 > T-1 收盘 | `gt("HIGH[0]", "CLOSE[1]")` | ✅ 已有 |
| 7 | 流通市值 < 150 亿 | `circ_mv_lt(150)` | 🆕 4a |
| 8 | T 日有下影线 | `has_lower_shadow(1.5, 0.02, 0)` | 🆕 4a |
| 9 | 近 8 日无 3 连板 | `no_consec_ups_in_window(3, 8)` | 🆕 4a |
| 10 | 近 30 日无跌停 | `no_limit_down_in_window(30)` | 🆕 4a |
| 11 | 近 90 日除 T-1 外有涨停 | `has_prior_limit_up(90, 1)` | 🆕 4a |

### 3.2 Pool 2（T+1 日收盘后，Pool 1 子集）

| # | 条件 | 积木 |
|---|---|---|
| 1 | T+1 实体上沿 < T 实体上沿 | `lt("BODY_UPPER[0]", "BODY_UPPER[1]")` |
| 2 | T+1 实体下沿 < T 实体下沿 | `lt("BODY_LOWER[0]", "BODY_LOWER[1]")` |
| 3 | T+1 有下影线 | `has_lower_shadow(1.5, 0.02, 0)` |

Pool 2 运行时仅在 Pool 1 前一日命中的 `ts_code` 范围内筛选。

### 3.3 监控（Week 5/6 范畴，此处仅记录规则）

对 Pool 2 标的，在 T+1 和 T+2 盘中监控：
- 回踩位 = `(当前价 - T-1 实体下沿) / (T-1 实体上沿 - T-1 实体下沿)`
- 触发阈值：< 40% / 30% / 20% 时分级告警

## 4. 数据层扩展（Week 4a）

### 4.1 daily_basic 表

新增 DuckDB 表 `daily_basic`，Tushare `daily_basic` 接口：

```sql
CREATE TABLE IF NOT EXISTS daily_basic (
    ts_code        VARCHAR NOT NULL,
    trade_date     DATE    NOT NULL,
    turnover_rate  DOUBLE,
    volume_ratio   DOUBLE,
    total_mv       DOUBLE,    -- 总市值（万元）
    circ_mv        DOUBLE,    -- 流通市值（万元）
    PRIMARY KEY (ts_code, trade_date)
);
```

入库方式：`DuckDBStore.upsert_daily_basic(df)` + `TushareAdapter.fetch_daily_basic(ts_code, start, end)`。

`ingest_daily.py` 在拉 daily_bar 后追加拉 daily_basic。

### 4.2 宽表扩展

**STATE_COLS_MAP 新增：**

| daily_state 列 | 宽表列名 |
|---|---|
| body_upper | `BODY_UPPER[n]` |
| body_lower | `BODY_LOWER[n]` |

**BASIC_COLS_MAP 新增**（loader.py 新增第四个 rename map）：

| daily_basic 列 | 宽表列名 |
|---|---|
| circ_mv | `CIRC_MV[n]` |
| total_mv | `TOTAL_MV[n]` |
| turnover_rate | `TURNOVER_RATE[n]` |

**派生列（规则内计算，不存储）：**
- 下影线 = `BODY_LOWER[n] - LOW[n]`
- 上影线 = `HIGH[n] - BODY_UPPER[n]`
- 实体高度 = `BODY_UPPER[n] - BODY_LOWER[n]`
- 振幅 = `(HIGH[n] - LOW[n]) / LOW[n]`

这些在积木函数内用 pandas 向量运算即时算，不占宽表列。

### 4.3 长窗口聚合列（动态方案 B）

规则声明所需窗口，load_universe 根据需求动态生成 SQL 聚合列。

**机制：**
- 规则通过 `_tag_lookback(fn, n)` 已有的 `min_lookback` 属性声明短期窗口需求
- 新增：规则可通过 `aggregate_requests` 属性声明长窗口聚合需求
- `screen()` 入口收集所有规则的 `aggregate_requests`，传给 `load_universe()`
- `load_universe()` 根据请求动态拼 SQL，结果作为非 `[n]` 后缀的普通列加到宽表

**聚合请求格式：**

```python
@dataclass
class AggregateRequest:
    name: str           # 结果列名，如 "max_consec_ups_8d"
    source_table: str   # "daily_state" | "daily_bar" | "daily_basic"
    source_col: str     # "consecutive_limit_ups" | "is_limit_down" 等
    agg_func: str       # "max" | "sum" | "any" | "count_nonzero"
    window: int         # 交易日窗口大小
    exclude_offset: int | None = None  # 排除某个 offset 的日期
```

**示例：**
- `no_consec_ups_in_window(3, 8)` 声明 `AggregateRequest(name="max_consec_ups_8d", source_table="daily_state", source_col="consecutive_limit_ups", agg_func="max", window=8)`
- 规则函数内检查 `df["max_consec_ups_8d"] < 3`

这样改窗口大小只改积木参数，不动 loader 代码。

## 5. 新积木清单（Week 4a）

### 5.1 下影线

```python
def has_lower_shadow(
    min_ratio: float = 1.5,
    min_amplitude: float = 0.02,
    offset: int = 0,
) -> Rule:
```

- 下影线 / 实体 ≥ `min_ratio`（默认 1.5，社区标准 2.0 = TA-Lib 锤子线）
- 振幅 ≥ `min_amplitude`（默认 2%，过滤十字星噪音）
- 实体为 0（一字线/十字星）时直接返回 False
- 计算：`lower_shadow = BODY_LOWER[offset] - LOW[offset]`，`body = BODY_UPPER[offset] - BODY_LOWER[offset]`

### 5.2 流通市值

```python
def circ_mv_lt(threshold_yi: float, offset: int = 0) -> Rule:
```

- 入参单位：亿元（用户习惯）
- 内部转换：Tushare circ_mv 单位是万元，`threshold_yi * 10000` 与 `CIRC_MV[offset]` 比较

### 5.3 非一字板

```python
def not_yiziban(offset: int = 0) -> Rule:
```

- `IS_YIZIBAN[offset] != True`（或 `IS_YIZIBAN[offset].fillna(False) == False`）

### 5.4 近 N 日无连续 M 个涨停

```python
def no_consec_ups_in_window(threshold: int = 3, window: int = 8) -> Rule:
```

- 声明 `AggregateRequest`：近 `window` 日 `consecutive_limit_ups` 的 max
- 规则：`max_value < threshold`
- `min_lookback = 0`（宽表短期窗口不需要，靠聚合列）

### 5.5 近 N 日无跌停

```python
def no_limit_down_in_window(window: int = 30) -> Rule:
```

- 声明 `AggregateRequest`：近 `window` 日 `is_limit_down` 的 any（bool or）
- 规则：`has_limit_down == False`

### 5.6 近 N 日有涨停（排除某日）

```python
def has_prior_limit_up(window: int = 90, exclude_offset: int = 1) -> Rule:
```

- 声明 `AggregateRequest`：近 `window` 日 `is_limit_up` 的 count，排除 offset=`exclude_offset` 的日期
- 规则：`count >= 1`

## 6. 调度层（Week 4b）

### 6.1 CLI 入口

`pyproject.toml` 新增：
```toml
[project.scripts]
rquant = "rquant.cli:main"
```

`src/rquant/cli.py` 用 argparse subcommands：

| 子命令 | 功能 | 参数 |
|---|---|---|
| `rquant serve` | APScheduler 常驻进程 | `--hour 17`（触发小时，默认 17） |
| `rquant run-daily` | 一次性全流水线 | `--date YYYY-MM-DD`（默认今天）`--preset NAME`（可选，只跑某个预设） |

### 6.2 APScheduler 配置

```python
from apscheduler.schedulers.blocking import BlockingScheduler

scheduler = BlockingScheduler()

@scheduler.scheduled_job("cron", hour=17, minute=0, day_of_week="mon-fri")
def daily_job():
    run_daily_pipeline(date.today())
```

- 新增依赖：`apscheduler>=3.10`
- Job store 用内存（MVP 不持久化）
- 信号处理：捕获 SIGINT/SIGTERM 优雅退出

### 6.3 每日流水线

`src/rquant/pipeline.py` 的 `run_daily_pipeline(trade_date)`:

```
① 检查是否交易日（查 daily_bar 该日有无数据；首次运行先拉数据再检查）
② Tushare 拉数据 → daily_bar + daily_basic
③ 算指标 → daily_indicator
④ 派生状态 → daily_state
⑤ 遍历 PRESET_SCREENS（按依赖拓扑排序）：
   ├─ 无 depends_on → screen(全市场) → upsert screen_result
   └─ 有 depends_on → 从 screen_result 查父预设 offset_days 天前的命中 → screen(子集) → upsert
⑥ 日志汇总：各预设命中数、耗时
```

### 6.4 screen_result 表

```sql
CREATE TABLE IF NOT EXISTS screen_result (
    trade_date    DATE    NOT NULL,
    preset_name   VARCHAR NOT NULL,
    ts_code       VARCHAR NOT NULL,
    name          VARCHAR,
    close         DOUBLE,
    pct_chg       DOUBLE,
    extra         JSON,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (trade_date, preset_name, ts_code)
);
```

- `extra` JSON：存 `include_columns` 指定的附加字段（如市值、下影线值、实体沿等）
- `DuckDBStore` 新增：`upsert_screen_result(df)` / `query_screen_result(trade_date, preset_name) -> pd.DataFrame`

### 6.5 预设注册表

```python
# src/rquant/presets.py

@dataclass
class ScreenPreset:
    name: str
    description: str
    rules: list[Rule]
    include_columns: list[str] | None = None
    depends_on: str | None = None
    offset_days: int = 0

PRESET_SCREENS: dict[str, ScreenPreset] = {
    "n-shape-pool1": ScreenPreset(
        name="N形态-Pool1",
        description="昨首板+安全过滤+下影线",
        rules=[
            not_st(),
            not_bj(),
            first_limit_up(offset=1),
            not_limit_up(offset=0),
            not_yiziban(offset=1),
            gt("HIGH[0]", "CLOSE[1]"),
            circ_mv_lt(150),
            has_lower_shadow(1.5, 0.02, 0),
            no_consec_ups_in_window(3, 8),
            no_limit_down_in_window(30),
            has_prior_limit_up(90, 1),
        ],
        include_columns=[
            "CIRC_MV[0]", "BODY_UPPER[0]", "BODY_LOWER[0]",
            "CONSECUTIVE_LIMIT_UPS[1]",
        ],
    ),
    "n-shape-pool2": ScreenPreset(
        name="N形态-Pool2",
        description="Pool1子集: T+1实体收缩+下影线",
        depends_on="n-shape-pool1",
        offset_days=1,
        rules=[
            lt("BODY_UPPER[0]", "BODY_UPPER[1]"),
            lt("BODY_LOWER[0]", "BODY_LOWER[1]"),
            has_lower_shadow(1.5, 0.02, 0),
        ],
        include_columns=[
            "BODY_UPPER[0]", "BODY_LOWER[0]", "BODY_UPPER[1]", "BODY_LOWER[1]",
        ],
    ),
}
```

### 6.6 screen() 扩展

`screen()` 新增可选参数：
```python
def screen(
    trade_date: str,
    rules: list[Rule],
    lookback: int | None = None,
    include_columns: list[str] | None = None,
    store: DuckDBStore | None = None,
    ts_code_whitelist: list[str] | None = None,  # 🆕 仅在这些股中筛
) -> pd.DataFrame:
```

## 7. 文件结构

### Week 4a 新增/修改
```
src/rquant/
├── screen/
│   ├── loader.py        # 修改：STATE_COLS_MAP + BASIC_COLS_MAP + 聚合列
│   └── rules.py         # 修改：6 个新积木 + AggregateRequest
├── storage/
│   ├── schema.py        # 修改：DAILY_BASIC_DDL
│   └── duckdb.py        # 修改：upsert_daily_basic()
├── adapter/
│   └── tushare.py       # 修改：fetch_daily_basic()
└── screen/__init__.py   # 修改：暴露新积木

tests/unit/
├── test_screen_rules.py     # 修改：新积木单测
└── test_screen_loader.py    # 修改：body_upper/circ_mv/聚合列
scripts/
└── ingest_daily.py          # 修改：追加 daily_basic 拉取
```

### Week 4b 新增/修改
```
src/rquant/
├── cli.py               # 新增：argparse serve/run-daily
├── presets.py            # 新增：ScreenPreset + PRESET_SCREENS
├── pipeline.py           # 新增：run_daily_pipeline() 全流水线
├── screen/
│   └── core.py           # 修改：screen() + ts_code_whitelist
└── storage/
    ├── schema.py          # 修改：SCREEN_RESULT_DDL
    └── duckdb.py          # 修改：upsert/query screen_result

tests/
├── unit/
│   ├── test_pipeline.py       # 新增：流水线单测（mock 各步骤）
│   └── test_screen_core.py    # 修改：whitelist 测试
├── integration/
│   └── test_preset_e2e.py     # 新增：N 形态预设端到端
└── smoke/
    └── smoke_pipeline.py      # 新增：真实数据冒烟
```

## 8. 测试策略

### Week 4a
- 每个新积木正/反例单测
- `has_lower_shadow`：构造精确 OHLC（如 O=10, H=11, L=8, C=10.5 → lower_shadow=2.5, body=0.5, ratio=5.0 → 命中）
- `circ_mv_lt`：验证万元→亿元单位转换
- 聚合列：DuckDB fixture 验证窗口 SQL
- loader 测试：body_upper/body_lower/circ_mv 在宽表中正确出现

### Week 4b
- pipeline 各步骤 mock 测试（不跑真实 Tushare）
- screen_result CRUD 测试
- 预设注册表加载测试
- Pool 2 依赖链：mock Pool 1 结果 → Pool 2 只在子集中筛
- CLI 入口冒烟（subprocess 调用验证 help/version）

### 端到端
- 用真实近 30-60 天数据跑 N 形态 Pool 1，验证命中结果合理性
- Pool 2 跑在 Pool 1 命中日的次日，验证子集筛选正确

## 9. 版本号

- Week 4a 完成 → `v0.3.1`（数据+积木补充，不改架构）
- Week 4b 完成 → `v0.4.0`（新增调度层，对外行为变化）

## 10. 风险与决策留痕

**风险：**
- 聚合列动态 SQL 拼接增加了 loader 复杂度 → 靠充分的单测覆盖
- Tushare `daily_basic` 可能有数据延迟（T 日数据要等盘后才有）→ 调度在 17:00 运行，通常已有数据
- APScheduler 内存 jobstore 重启后不恢复漏跑的 job → MVP 可接受，手动 `run-daily` 补

**已放弃的选项：**
- ❌ 聚合列硬编码 SQL（方案 A）：改窗口要改 loader，不符合"改个数字"的目标
- ❌ 存 upper_shadow/lower_shadow/body_range 到 daily_state：可从现有列即时算出，不增加存储
- ❌ YAML/JSON 配置预设：违反 Week 3b 决策（不走 DSL），Python 文件即声明
- ❌ Celery / Airflow：个人项目杀鸡用牛刀

## 11. Definition of Done

### Week 4a
- [ ] daily_basic 表建好，ingest_daily.py 能拉 circ_mv 等字段
- [ ] 宽表正确暴露 BODY_UPPER[n] / BODY_LOWER[n] / CIRC_MV[n]
- [ ] 6 个新积木全部有单测覆盖
- [ ] 聚合列动态生成可工作（至少覆盖 max / any / count 三种 agg_func）
- [ ] CHANGELOG 追加 [v0.3.1] 条目
- [ ] tag v0.3.1

### Week 4b
- [ ] `rquant serve` 能启动 APScheduler 常驻
- [ ] `rquant run-daily --date X` 能跑完整流水线
- [ ] screen_result 表落库成功，可查询
- [ ] N 形态 Pool 1 + Pool 2 预设注册并可运行
- [ ] Pool 2 正确限定在 Pool 1 命中子集中筛
- [ ] 近期真实数据冒烟通过
- [ ] CHANGELOG 追加 [v0.4.0] 条目
- [ ] tag v0.4.0
