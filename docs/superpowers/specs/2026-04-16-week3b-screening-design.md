---
title: Week 3b 筛选规则引擎 Design
created_at: 2026-04-16
status: draft
owner: roxor
---

# Week 3b — 筛选规则引擎（Screening Engine）

## 1. 背景与动机

Week 3a 完成了派生字段层（`daily_state` 15 列：涨跌停 / 首板 / 一字板 / 连板 / 板块 / ST / 实体上下沿）。Week 3b 在此基础上做**筛选引擎**：

> **核心能力**：给定某个交易日 + 一组条件 → 返回符合条件的股票 `ts_code` 列表 + 关键字段。

真实使用场景示例（用户原话）：
> "过滤 ST，过滤北交所，昨天首板，今天未涨停，今天最高价大于昨天收盘价"

该场景涉及 5 个条件、跨 T 和 T-1 两天。因此单条硬编码函数不够，必须支持**多条件组合 + 跨日引用**。

## 2. 范围边界

**Week 3b 做的**：
- 一套可组合的「原子条件积木」函数库（Python）
- 一个入口 `screen()` 把积木列表作用于某个交易日的全市场数据
- 覆盖用户示例场景 + 常见选股形态（首板次日、MA 金叉、连板、突破等）
- 积木命名对齐 **通达信 / MyTT 风格**（为 Week 8 通达信代码支持铺路）

**Week 3b 不做**（留给后续阶段）：
- ❌ DSL / YAML / JSON 规则配置化（违反 README 开放决策"MVP 先用函数，UI 化时再抽 DSL"）
- ❌ CLI 命令 `rquant screen ...`（Week 4 调度时一起做）
- ❌ 筛选结果落库 `screen_result` 表（Week 4 调度时一起做）
- ❌ OR / NOT / 复杂布尔树（Week 3b 只做 AND 组合，够覆盖示例场景）
- ❌ 自然语言输入 / 通达信代码解析（Week 7/8）

## 3. 使用形态

### 3.1 Python 调用（Week 3b 的主形态）

```python
from rquant.screen import screen
from rquant.screen.rules import (
    not_st, not_bj, first_limit_up, not_limit_up, gt,
)

results = screen(
    trade_date="2026-04-15",
    rules=[
        not_st(),                                 # 今日非 ST
        not_bj(),                                 # 今日非北交所
        first_limit_up(offset=1),                 # 昨日首板
        not_limit_up(offset=0),                   # 今日未涨停
        gt(left="HIGH[0]", right="CLOSE[1]"),     # 今高 > 昨收
    ],
)
# results: pd.DataFrame，列：ts_code, name, close, pct_chg, ... 命中条件摘要
```

### 3.2 对 Week 7 GUI 的映射预演

每块积木 = GUI 一个勾选项 / 下拉项。用户在 GUI 界面勾选后生成一段 Python 代码（或直接调用），和 3.1 等价。这就是为什么**不在 Week 3b 做 DSL**：函数本身已经够像声明，GUI 层直接映射到函数调用。

## 4. 数据加载层

### 4.1 宽表结构

`load_universe(trade_date, lookback=5)` 返回一个 pandas DataFrame：

| 列名 | 含义 | 来源 |
|---|---|---|
| `ts_code` | 股票代码 | daily_bar |
| `name` | 股票名 | stock_basic |
| `CLOSE[0]`, `CLOSE[1]`, ... `CLOSE[lookback]` | T 日、T-1 日、... 的收盘价 | daily_bar |
| `HIGH[0]`, `HIGH[1]`, ... | 最高价 | daily_bar |
| `LOW[0]`, `LOW[1]`, ... | 最低价 | daily_bar |
| `OPEN[0]`, `OPEN[1]`, ... | 开盘价 | daily_bar |
| `VOL[0]`, `VOL[1]`, ... | 成交量 | daily_bar |
| `AMOUNT[0]`, ... | 成交额 | daily_bar |
| `PCT_CHG[0]`, ... | 涨跌幅 | daily_bar |
| `PRE_CLOSE[0]`, ... | 前收盘价（涨跌停计算用） | daily_bar |
| `MA5[0]`, `MA20[0]`, ... | 均线 | daily_indicator |
| `RSI14[0]`, `MACD[0]`, `KDJ_K[0]`, ... | 其他指标 | daily_indicator |
| `is_st`, `is_bj`, `board_type` | 当前属性（不分日） | daily_state (T 日) + stock_basic |
| `IS_LIMIT_UP[0]`, `IS_LIMIT_UP[1]`, ... | 涨停状态 | daily_state |
| `IS_FIRST_LIMIT_UP[0]`, `IS_FIRST_LIMIT_UP[1]`, ... | 首板 | daily_state |
| `IS_YIZIBAN[0]`, ... | 一字板 | daily_state |
| `CONSECUTIVE_LIMIT_UPS[0]`, ... | 连板数 | daily_state |

**命名约定**（Week 8 通达信映射关键）：
- 价量字段用**通达信大写风格** `CLOSE/HIGH/LOW/OPEN/VOL/AMOUNT/PCT_CHG/PRE_CLOSE`
- 指标字段用通达信习惯 `MA5/MA20/RSI14/MACD/KDJ_K/KDJ_D/KDJ_J`
- 状态字段沿用 daily_state 的大写版 `IS_LIMIT_UP/IS_FIRST_LIMIT_UP/IS_YIZIBAN/CONSECUTIVE_LIMIT_UPS`
- 跨日用 `[n]` 后缀，`[0]` = 今日，`[1]` = 昨日，`[n]` = n 日前（≈ 通达信 `REF(X, n)`）
- 不带 `[n]` 的字段（`is_st/is_bj/board_type/name`）是不分日的属性

**lookback 默认 5 天**，覆盖绝大多数规则需求（均线/连板/昨首板等）。个别规则需要更长（如 60 日突破）时，规则函数自己声明 `min_lookback`，`screen()` 取所有规则 `min_lookback` 的最大值。

### 4.2 实现

内部用 DuckDB SQL 把 `daily_bar / daily_indicator / daily_state / stock_basic` 按交易日历 JOIN，转成上面的宽表。一次拉全市场 ~5500 行 × 几十列 × 5 天 ≈ 几 MB 内存，无压力。

## 5. 规则积木清单（Week 3b 交付集）

按"覆盖用户示例 + 最常见选股形态"定义。每块积木是一个返回 `pd.Series[bool]` 的工厂函数。

**`offset` 语义**：`offset=n` 表示 T-n 日（今日 = 0，昨日 = 1，n 日前 = n）。所有带 `offset` 参数的积木都遵循此约定。

### 5.1 属性类（不涉及时间）
| 积木 | 含义 | 对应通达信习惯 |
|---|---|---|
| `not_st()` | 排除 ST | `NAMELIKE('ST')=0` |
| `not_bj()` | 排除北交所（= `board_in(["main","gem","star"])` 的快捷方式） | 板块过滤 |
| `board_in(["main","gem"])` | 板块白名单（可选值：`main/gem/star/bj`） | — |

### 5.2 涨跌停 / 连板类（`offset` 支持跨日）
| 积木 | 含义 |
|---|---|
| `limit_up(offset=0)` | 某日涨停 |
| `not_limit_up(offset=0)` | 某日未涨停 |
| `first_limit_up(offset=0)` | 某日首板 |
| `yiziban(offset=0)` | 某日一字板 |
| `consecutive_ups_gte(n, offset=0)` | 某日连板数 ≥ n |
| `limit_down(offset=0)` | 某日跌停 |

### 5.3 价量比较类
| 积木 | 含义 |
|---|---|
| `gt(left, right)` | 任意字段 > 任意字段（支持常数） |
| `lt(left, right)` | 小于 |
| `gte(left, right)` | 大于等于 |
| `lte(left, right)` | 小于等于 |
| `between(field, low, high)` | 范围 |

`left/right` 用字符串表达式字段名，如 `"CLOSE[0]"`、`"MA20[0]"`、`"PRE_CLOSE[1]"`；常数直接用数字。

### 5.4 均线 / 指标类
| 积木 | 含义 |
|---|---|
| `cross_above(fast, slow, offset=0)` | fast 均线上穿 slow（今日上穿 = `offset=0`） |
| `cross_below(fast, slow, offset=0)` | 下穿 |
| `above_ma(period, offset=0)` | `CLOSE > MAn` |
| `rsi_oversold(period=14, threshold=30)` | RSI 超卖 |
| `rsi_overbought(period=14, threshold=70)` | RSI 超买 |

### 5.5 成交量类
| 积木 | 含义 |
|---|---|
| `volume_ratio_gte(n, offset=0)` | 量比 ≥ n（vs 前 5 日均量） |

### 5.6 入口 API
```python
def screen(
    trade_date: str | datetime.date,
    rules: list[Callable[[pd.DataFrame], pd.Series]],
    lookback: int | None = None,  # None 表示按 rules 最大需求自动推断
    include_columns: list[str] | None = None,  # 结果额外带哪些列
) -> pd.DataFrame:
    ...
```

**组合逻辑**：rules 列表内部用 AND 合并。OR/NOT 不做。

**返回结构**：
- 必带列：`ts_code, name, CLOSE[0], PCT_CHG[0]`
- `include_columns` 控制附加列
- 行数 = 命中股票数，按 `ts_code` 升序

## 6. 测试策略

用 pytest + fixture DataFrame（手工构造的小宽表，~10 行覆盖典型情形）。

**必测场景**：
1. 用户原始场景：昨首板 + 今未涨停 + 今高>昨收 + 非 ST + 非北交所
2. 每块积木单独测正/反例
3. 跨日引用正确性（`[0]` / `[1]` 不错位）
4. `lookback` 自动推断（规则要 20 日均线时 lookback 应 ≥ 20）
5. 空结果处理（没有股票命中时返回空 DataFrame，不报错）
6. 积木可复用性（同一积木用不同 offset 不互相干扰）

**端到端验证**：
- 用 Week 3a 已验证过的样本日期（2024 年 9-11 月赛力斯涨停季）跑一遍，确认积木能找到已知命中结果

## 7. 文件结构

```
src/rquant/
├── screen/
│   ├── __init__.py          # 暴露 screen()
│   ├── loader.py            # load_universe() 加载宽表
│   ├── rules.py             # 所有积木函数
│   └── core.py              # screen() 实现（AND 合并 + 调度）
└── ...

tests/
└── screen/
    ├── test_rules.py        # 积木单测
    ├── test_loader.py       # 宽表加载
    └── test_screen.py       # 端到端（含用户原始场景）
```

## 8. 对后续周次的影响

### Week 7（原：Streamlit 最小 UI）→ **改为：Streamlit UI + NL 输入**
- Streamlit 面板保留：备选池 / 告警历史 / 规则开关
- 新增：一个文本框，用户输入自然语言（"昨天首板今天没涨停且高点比昨收盘高"），走 LLM → 积木调用 → 跑 `screen()`
- LLM prompt 里注入积木清单和签名即可，工程量小

### Week 8（新增）→ **通达信代码支持**
- GUI 新增"贴通达信公式"输入框
- 实现一个通达信公式解析器（选股公式子集），把表达式映射到：
  - 价量函数 → MyTT（已在用，函数名天然对齐）
  - 状态判断 → daily_state 字段
  - 组合逻辑 → 积木调用或直接 pandas 布尔运算
- 工作量大，独立成周

### 对 README / CLAUDE.md / Changelog 的更新
- README MVP 路径 7 周 → 8 周，Week 7/8 重写
- README 开放决策 "UI 化时再抽 DSL" 补脚注：实际路径是 NL + 通达信，不是 YAML DSL
- 项目 CLAUDE.md MVP 路径同步扩为 8 周
- Memory `project_rquant.md` 同步

## 9. 风险与决策留痕

**风险**：
- 用字符串表达式（如 `"CLOSE[0]"`）给 `gt/lt` 等积木，类型系统不帮忙查错 → 靠单测覆盖
- 宽表列名和 daily_state 小写字段有双命名（`is_limit_up` vs `IS_LIMIT_UP[0]`）→ 宽表只用大写 + `[n]` 后缀，不引入小写版，避免混乱

**已放弃的选项**：
- ❌ YAML DSL：违反 README 决策，增加解析层没有用户价值
- ❌ SQL-only（把所有条件塞进 SQL WHERE）：跨日 JOIN 表达复杂规则难读难测，不利于 GUI 映射
- ❌ OR / NOT 组合：Week 3b 暂不需要，示例场景全 AND 即可；要的话积木内部已经能用 `~` 表达 NOT（如 `not_limit_up` = `~is_limit_up`）

## 10. Definition of Done

- [ ] `src/rquant/screen/` 三个模块实现完
- [ ] 5.1–5.5 的所有积木 + 入口 `screen()` 有单测覆盖
- [ ] 用户原始场景端到端跑通，能在真实 2024-2026 年数据上返回合理结果
- [ ] `CHANGELOG.md` 追加 [v0.3.0] Week 3b 条目
- [ ] README / 项目 CLAUDE.md / Memory 同步更新 Week 7/8
- [ ] tag `v0.3.0` 打上，合回 main
