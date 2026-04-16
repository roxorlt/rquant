# Week 3b — Screening Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 搭一套「原子条件积木」函数库 + `screen()` 入口，把给定交易日 + 一组条件 → 符合条件的 A 股列表，这件事做成一条 Python 调用。

**Architecture:** `src/rquant/screen/` 三个模块：`loader.py` 一次拉 DuckDB 宽表，`rules.py` 是所有积木函数（闭包 + `min_lookback` 属性），`core.py` 把 rules 列表 AND 合并后应用在宽表上返回结果。命名对齐通达信/MyTT 风格（`CLOSE[0]` / `MA20[0]` / `IS_LIMIT_UP[1]`）为 Week 8 通达信代码支持铺路。

**Tech Stack:** Python 3.12+ / pandas / DuckDB / MyTT / pytest + pytest.mark.parametrize。

---

## File Structure

```
src/rquant/screen/
├── __init__.py          # 暴露 screen, load_universe, 常用 rules
├── loader.py            # load_universe() + _resolve_trading_dates()
├── rules.py             # 所有积木（not_st, first_limit_up, gt, cross_above, ...）
└── core.py              # screen() 主流程

tests/fixtures/
├── __init__.py
└── wide_frames.py       # make_wide_frame() 生成测试宽表

tests/unit/
├── test_screen_loader.py
├── test_screen_rules.py
└── test_screen_core.py

tests/integration/
└── test_screen_e2e.py   # 用户原始场景端到端
```

宽表列命名约定（loader 产出、rules 消费，单一来源）：

| 前缀/格式 | 含义 | 示例 |
|---|---|---|
| `CLOSE[n]` / `OPEN[n]` / `HIGH[n]` / `LOW[n]` / `VOL[n]` / `AMOUNT[n]` / `PCT_CHG[n]` / `PRE_CLOSE[n]` | T-n 日价量 | `CLOSE[0]` 今收 |
| `MA5[n]` / `MA10[n]` / `MA20[n]` / `MA60[n]` | 均线 | `MA20[0]` |
| `RSI6[n]` / `RSI14[n]` / `MACD[n]` / `MACD_SIGNAL[n]` / `MACD_HIST[n]` / `KDJ_K[n]` / `KDJ_D[n]` / `KDJ_J[n]` | 其他指标 | — |
| `IS_LIMIT_UP[n]` / `IS_LIMIT_DOWN[n]` / `IS_FIRST_LIMIT_UP[n]` / `IS_YIZIBAN[n]` / `CONSECUTIVE_LIMIT_UPS[n]` | 状态 | — |
| `is_st` / `is_bj` / `board_type` / `ts_code` / `name` | 不分日属性 | 取 T=0 日的值 |

所有积木函数带 `min_lookback: int` 属性，`screen()` 根据 rules 列表自动推断最大 lookback。

---

### Task 1: 模块骨架 + 测试 fixture

**Files:**
- Create: `src/rquant/screen/__init__.py`
- Create: `src/rquant/screen/loader.py`
- Create: `src/rquant/screen/rules.py`
- Create: `src/rquant/screen/core.py`
- Create: `tests/fixtures/__init__.py`
- Create: `tests/fixtures/wide_frames.py`

- [ ] **Step 1: 建空模块 + 最小 `__init__.py`**

`src/rquant/screen/__init__.py`:
```python
"""筛选引擎：积木函数 + screen() 入口。"""

from rquant.screen.core import screen
from rquant.screen.loader import load_universe

__all__ = ["screen", "load_universe"]
```

`src/rquant/screen/loader.py`:
```python
"""宽表加载：把 daily_bar/daily_indicator/daily_state 合并成
每行 1 只股票、每字段带 [n] 后缀的宽表。"""

from __future__ import annotations

import pandas as pd

from rquant.storage.duckdb import DuckDBStore


def load_universe(
    trade_date: str,
    lookback: int = 5,
    store: DuckDBStore | None = None,
) -> pd.DataFrame:
    raise NotImplementedError
```

`src/rquant/screen/rules.py`:
```python
"""筛选积木：每块是返回 (df) -> pd.Series[bool] 的工厂函数。"""

from __future__ import annotations

from typing import Callable

import pandas as pd

Rule = Callable[[pd.DataFrame], pd.Series]
```

`src/rquant/screen/core.py`:
```python
"""screen() 主流程：加载宽表 → 应用规则 → 返回结果。"""

from __future__ import annotations

from typing import Callable

import pandas as pd

from rquant.screen.loader import load_universe

Rule = Callable[[pd.DataFrame], pd.Series]


def screen(
    trade_date: str,
    rules: list[Rule],
    lookback: int | None = None,
    include_columns: list[str] | None = None,
    store=None,
) -> pd.DataFrame:
    raise NotImplementedError
```

- [ ] **Step 2: 建 fixture 生成器**

`tests/fixtures/__init__.py`:
```python
"""测试 fixture 辅助。"""
```

`tests/fixtures/wide_frames.py`:
```python
"""宽表 DataFrame 构造器，用于积木单测。"""

from __future__ import annotations

import pandas as pd

# 默认 5 只股票、lookback=3 的迷你宽表
DEFAULT_CODES = ["000001.SZ", "300001.SZ", "688001.SH", "833001.BJ", "600001.SH"]


def make_wide_frame(
    codes: list[str] | None = None,
    lookback: int = 3,
    overrides: dict | None = None,
) -> pd.DataFrame:
    """构造一个宽表 DataFrame。

    默认每只股票每字段全填 0.0（或 False），lookback+1 天。
    overrides 按 {(ts_code, 列名): value} 局部覆盖。

    - `CLOSE[n]`, `OPEN[n]`, `HIGH[n]`, `LOW[n]`, `VOL[n]`, `PCT_CHG[n]`, `PRE_CLOSE[n]`
    - `MA5[n]`, `MA20[n]`, `MA60[n]`, `RSI14[n]`, `MACD[n]`
    - `IS_LIMIT_UP[n]`, `IS_LIMIT_DOWN[n]`, `IS_FIRST_LIMIT_UP[n]`, `IS_YIZIBAN[n]`,
      `CONSECUTIVE_LIMIT_UPS[n]`
    - `is_st`, `is_bj`, `board_type`, `ts_code`, `name`
    """
    codes = codes or DEFAULT_CODES
    price_cols = ["CLOSE", "OPEN", "HIGH", "LOW", "VOL", "PCT_CHG", "PRE_CLOSE", "AMOUNT"]
    ind_cols = [
        "MA5", "MA10", "MA20", "MA60",
        "RSI6", "RSI14",
        "MACD", "MACD_SIGNAL", "MACD_HIST",
        "KDJ_K", "KDJ_D", "KDJ_J",
    ]
    bool_state_cols = [
        "IS_LIMIT_UP", "IS_LIMIT_DOWN", "IS_FIRST_LIMIT_UP", "IS_YIZIBAN",
    ]
    int_state_cols = ["CONSECUTIVE_LIMIT_UPS"]

    rows = []
    for code in codes:
        row = {"ts_code": code, "name": code.split(".")[0]}
        row["is_st"] = False
        row["is_bj"] = code.endswith(".BJ")
        if code.startswith("300") or code.startswith("301"):
            row["board_type"] = "gem"
        elif code.startswith("688") or code.startswith("689"):
            row["board_type"] = "star"
        elif code.endswith(".BJ"):
            row["board_type"] = "bj"
        else:
            row["board_type"] = "main"

        for n in range(lookback + 1):
            for c in price_cols:
                row[f"{c}[{n}]"] = 0.0
            for c in ind_cols:
                row[f"{c}[{n}]"] = 0.0
            for c in bool_state_cols:
                row[f"{c}[{n}]"] = False
            for c in int_state_cols:
                row[f"{c}[{n}]"] = 0
        rows.append(row)

    df = pd.DataFrame(rows)

    if overrides:
        for (code, col), value in overrides.items():
            df.loc[df["ts_code"] == code, col] = value

    return df
```

- [ ] **Step 3: Smoke test — fixture 能用**

`tests/unit/test_screen_rules.py`:
```python
"""筛选积木单测。"""

import pandas as pd

from tests.fixtures.wide_frames import make_wide_frame


class TestFixture:
    def test_default_frame_has_expected_columns(self) -> None:
        df = make_wide_frame(lookback=3)
        assert "CLOSE[0]" in df.columns
        assert "CLOSE[3]" in df.columns
        assert "IS_FIRST_LIMIT_UP[1]" in df.columns
        assert "is_st" in df.columns
        assert "board_type" in df.columns
        assert len(df) == 5

    def test_overrides_apply(self) -> None:
        df = make_wide_frame(
            lookback=1,
            overrides={("300001.SZ", "CLOSE[0]"): 42.0},
        )
        assert df.loc[df["ts_code"] == "300001.SZ", "CLOSE[0]"].iloc[0] == 42.0
        assert df.loc[df["ts_code"] == "000001.SZ", "CLOSE[0]"].iloc[0] == 0.0

    def test_board_type_detection(self) -> None:
        df = make_wide_frame()
        board = dict(zip(df["ts_code"], df["board_type"]))
        assert board["000001.SZ"] == "main"
        assert board["300001.SZ"] == "gem"
        assert board["688001.SH"] == "star"
        assert board["833001.BJ"] == "bj"
```

- [ ] **Step 4: 运行，确认 fixture 测试通过**

Run: `uv run pytest tests/unit/test_screen_rules.py -v`
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add src/rquant/screen tests/fixtures tests/unit/test_screen_rules.py
git commit -m "feat(screen): scaffold screen module + test fixtures"
```

---

### Task 2: 属性类积木（`not_st` / `not_bj` / `board_in`）

**Files:**
- Modify: `src/rquant/screen/rules.py`
- Modify: `tests/unit/test_screen_rules.py`

- [ ] **Step 1: 写失败的测试**

在 `tests/unit/test_screen_rules.py` 追加：
```python
import pytest

from rquant.screen.rules import board_in, not_bj, not_st


class TestAttributeRules:
    def test_not_st_excludes_st_stocks(self) -> None:
        df = make_wide_frame(overrides={("000001.SZ", "is_st"): True})
        mask = not_st()(df)
        assert not mask.loc[df["ts_code"] == "000001.SZ"].iloc[0]
        assert mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]

    def test_not_bj_excludes_bj_stocks(self) -> None:
        df = make_wide_frame()
        mask = not_bj()(df)
        assert not mask.loc[df["ts_code"] == "833001.BJ"].iloc[0]
        assert mask.loc[df["ts_code"] == "000001.SZ"].iloc[0]

    @pytest.mark.parametrize(
        "whitelist,expected_allowed,expected_blocked",
        [
            (["main"], "000001.SZ", "300001.SZ"),
            (["main", "gem"], "300001.SZ", "688001.SH"),
            (["bj"], "833001.BJ", "000001.SZ"),
        ],
    )
    def test_board_in_whitelist(
        self, whitelist: list[str], expected_allowed: str, expected_blocked: str
    ) -> None:
        df = make_wide_frame()
        mask = board_in(whitelist)(df)
        assert mask.loc[df["ts_code"] == expected_allowed].iloc[0]
        assert not mask.loc[df["ts_code"] == expected_blocked].iloc[0]

    def test_attribute_rules_min_lookback_is_zero(self) -> None:
        assert not_st().min_lookback == 0
        assert not_bj().min_lookback == 0
        assert board_in(["main"]).min_lookback == 0
```

- [ ] **Step 2: 运行，确认失败**

Run: `uv run pytest tests/unit/test_screen_rules.py::TestAttributeRules -v`
Expected: ImportError — rules.py 里还没有 `not_st/not_bj/board_in`

- [ ] **Step 3: 实现积木**

追加到 `src/rquant/screen/rules.py`（在 `Rule` type alias 之后）：
```python
def _tag_lookback(fn: Rule, n: int) -> Rule:
    """给规则函数挂上 min_lookback 属性，方便 screen() 推断总 lookback。"""
    fn.min_lookback = n  # type: ignore[attr-defined]
    return fn


def not_st() -> Rule:
    """排除 ST / *ST / SST。"""
    def _rule(df: pd.DataFrame) -> pd.Series:
        return ~df["is_st"].astype(bool)
    return _tag_lookback(_rule, 0)


def not_bj() -> Rule:
    """排除北交所（= board_in(['main','gem','star']) 的快捷方式）。"""
    def _rule(df: pd.DataFrame) -> pd.Series:
        return ~df["is_bj"].astype(bool)
    return _tag_lookback(_rule, 0)


def board_in(boards: list[str]) -> Rule:
    """板块白名单，boards 可选值 main / gem / star / bj。"""
    allowed = set(boards)
    def _rule(df: pd.DataFrame) -> pd.Series:
        return df["board_type"].isin(allowed)
    return _tag_lookback(_rule, 0)
```

- [ ] **Step 4: 运行测试通过**

Run: `uv run pytest tests/unit/test_screen_rules.py::TestAttributeRules -v`
Expected: 6 passed

- [ ] **Step 5: Commit**

```bash
git add src/rquant/screen/rules.py tests/unit/test_screen_rules.py
git commit -m "feat(screen): add attribute rules (not_st, not_bj, board_in)"
```

---

### Task 3: 涨跌停 / 连板类积木

**Files:**
- Modify: `src/rquant/screen/rules.py`
- Modify: `tests/unit/test_screen_rules.py`

积木清单：`limit_up(offset=0)` / `not_limit_up(offset=0)` / `first_limit_up(offset=0)` / `yiziban(offset=0)` / `consecutive_ups_gte(n, offset=0)` / `limit_down(offset=0)`。

- [ ] **Step 1: 写失败的测试**

追加到 `tests/unit/test_screen_rules.py`：
```python
from rquant.screen.rules import (
    consecutive_ups_gte,
    first_limit_up,
    limit_down,
    limit_up,
    not_limit_up,
    yiziban,
)


class TestLimitRules:
    def test_limit_up_today(self) -> None:
        df = make_wide_frame(overrides={("300001.SZ", "IS_LIMIT_UP[0]"): True})
        mask = limit_up(offset=0)(df)
        assert mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert not mask.loc[df["ts_code"] == "000001.SZ"].iloc[0]

    def test_limit_up_yesterday(self) -> None:
        df = make_wide_frame(
            lookback=2,
            overrides={("300001.SZ", "IS_LIMIT_UP[1]"): True},
        )
        mask = limit_up(offset=1)(df)
        assert mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert limit_up(offset=1).min_lookback == 1

    def test_not_limit_up(self) -> None:
        df = make_wide_frame(overrides={("300001.SZ", "IS_LIMIT_UP[0]"): True})
        mask = not_limit_up(offset=0)(df)
        assert not mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert mask.loc[df["ts_code"] == "000001.SZ"].iloc[0]

    def test_first_limit_up(self) -> None:
        df = make_wide_frame(
            overrides={("300001.SZ", "IS_FIRST_LIMIT_UP[0]"): True},
        )
        mask = first_limit_up(offset=0)(df)
        assert mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert not mask.loc[df["ts_code"] == "000001.SZ"].iloc[0]

    def test_yiziban(self) -> None:
        df = make_wide_frame(overrides={("300001.SZ", "IS_YIZIBAN[0]"): True})
        mask = yiziban(offset=0)(df)
        assert mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]

    def test_consecutive_ups_gte(self) -> None:
        df = make_wide_frame(
            overrides={
                ("300001.SZ", "CONSECUTIVE_LIMIT_UPS[0]"): 2,
                ("000001.SZ", "CONSECUTIVE_LIMIT_UPS[0]"): 1,
            }
        )
        mask = consecutive_ups_gte(2)(df)
        assert mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert not mask.loc[df["ts_code"] == "000001.SZ"].iloc[0]

    def test_limit_down(self) -> None:
        df = make_wide_frame(overrides={("000001.SZ", "IS_LIMIT_DOWN[0]"): True})
        mask = limit_down(offset=0)(df)
        assert mask.loc[df["ts_code"] == "000001.SZ"].iloc[0]
```

- [ ] **Step 2: 运行，确认失败**

Run: `uv run pytest tests/unit/test_screen_rules.py::TestLimitRules -v`
Expected: ImportError

- [ ] **Step 3: 实现积木**

追加到 `src/rquant/screen/rules.py`：
```python
def _bool_state_rule(col_base: str, offset: int, negate: bool = False) -> Rule:
    col = f"{col_base}[{offset}]"
    def _rule(df: pd.DataFrame) -> pd.Series:
        s = df[col].fillna(False).astype(bool)
        return ~s if negate else s
    return _tag_lookback(_rule, offset)


def limit_up(offset: int = 0) -> Rule:
    """某日涨停。"""
    return _bool_state_rule("IS_LIMIT_UP", offset)


def not_limit_up(offset: int = 0) -> Rule:
    """某日未涨停。"""
    return _bool_state_rule("IS_LIMIT_UP", offset, negate=True)


def first_limit_up(offset: int = 0) -> Rule:
    """某日首板（今涨停且昨未涨停）。"""
    return _bool_state_rule("IS_FIRST_LIMIT_UP", offset)


def yiziban(offset: int = 0) -> Rule:
    """某日一字板。"""
    return _bool_state_rule("IS_YIZIBAN", offset)


def limit_down(offset: int = 0) -> Rule:
    """某日跌停。"""
    return _bool_state_rule("IS_LIMIT_DOWN", offset)


def consecutive_ups_gte(n: int, offset: int = 0) -> Rule:
    """某日连板数 ≥ n。"""
    col = f"CONSECUTIVE_LIMIT_UPS[{offset}]"
    def _rule(df: pd.DataFrame) -> pd.Series:
        return df[col].fillna(0).astype(int) >= n
    return _tag_lookback(_rule, offset)
```

- [ ] **Step 4: 运行测试通过**

Run: `uv run pytest tests/unit/test_screen_rules.py::TestLimitRules -v`
Expected: 7 passed

- [ ] **Step 5: Commit**

```bash
git add src/rquant/screen/rules.py tests/unit/test_screen_rules.py
git commit -m "feat(screen): add limit-up family rules (limit_up/first_limit_up/yiziban/consecutive_ups_gte/limit_down)"
```

---

### Task 4: 价量比较类积木（`gt` / `lt` / `gte` / `lte` / `between`）

**Files:**
- Modify: `src/rquant/screen/rules.py`
- Modify: `tests/unit/test_screen_rules.py`

`gt(left, right)` 两参数都支持字段名字符串（如 `"HIGH[0]"`）或数字常数。内部用正则抽取 `[n]` 算 min_lookback。

- [ ] **Step 1: 写失败的测试**

追加到 `tests/unit/test_screen_rules.py`：
```python
from rquant.screen.rules import between, gt, gte, lt, lte


class TestCompareRules:
    def test_gt_field_vs_field_cross_day(self) -> None:
        # 300001.SZ 今高 > 昨收
        df = make_wide_frame(
            lookback=2,
            overrides={
                ("300001.SZ", "HIGH[0]"): 12.0,
                ("300001.SZ", "CLOSE[1]"): 10.0,
                ("000001.SZ", "HIGH[0]"): 10.0,
                ("000001.SZ", "CLOSE[1]"): 12.0,
            },
        )
        rule = gt("HIGH[0]", "CLOSE[1]")
        mask = rule(df)
        assert mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert not mask.loc[df["ts_code"] == "000001.SZ"].iloc[0]
        assert rule.min_lookback == 1

    def test_gt_field_vs_constant(self) -> None:
        df = make_wide_frame(
            overrides={("300001.SZ", "CLOSE[0]"): 15.0, ("000001.SZ", "CLOSE[0]"): 5.0},
        )
        rule = gt("CLOSE[0]", 10.0)
        mask = rule(df)
        assert mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert not mask.loc[df["ts_code"] == "000001.SZ"].iloc[0]
        assert rule.min_lookback == 0

    def test_lt(self) -> None:
        df = make_wide_frame(
            overrides={("000001.SZ", "CLOSE[0]"): 5.0, ("300001.SZ", "CLOSE[0]"): 15.0},
        )
        mask = lt("CLOSE[0]", 10.0)(df)
        assert mask.loc[df["ts_code"] == "000001.SZ"].iloc[0]
        assert not mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]

    def test_gte_boundary(self) -> None:
        df = make_wide_frame(overrides={("000001.SZ", "CLOSE[0]"): 10.0})
        assert gte("CLOSE[0]", 10.0)(df).loc[df["ts_code"] == "000001.SZ"].iloc[0]

    def test_lte_boundary(self) -> None:
        df = make_wide_frame(overrides={("000001.SZ", "CLOSE[0]"): 10.0})
        assert lte("CLOSE[0]", 10.0)(df).loc[df["ts_code"] == "000001.SZ"].iloc[0]

    def test_between(self) -> None:
        df = make_wide_frame(
            overrides={
                ("000001.SZ", "CLOSE[0]"): 8.0,
                ("300001.SZ", "CLOSE[0]"): 15.0,
                ("688001.SH", "CLOSE[0]"): 25.0,
            },
        )
        mask = between("CLOSE[0]", 10.0, 20.0)(df)
        assert not mask.loc[df["ts_code"] == "000001.SZ"].iloc[0]
        assert mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert not mask.loc[df["ts_code"] == "688001.SH"].iloc[0]
```

- [ ] **Step 2: 运行，确认失败**

Run: `uv run pytest tests/unit/test_screen_rules.py::TestCompareRules -v`
Expected: ImportError

- [ ] **Step 3: 实现积木**

在 `src/rquant/screen/rules.py` 顶部加 `import re`，然后追加：
```python
_LOOKBACK_RE = re.compile(r"\[(\d+)\]$")


def _parse_lookback(operand: str | float | int) -> int:
    """从 'CLOSE[3]' 抽出 3；数字常数返回 0。"""
    if isinstance(operand, (int, float)):
        return 0
    match = _LOOKBACK_RE.search(operand)
    return int(match.group(1)) if match else 0


def _resolve(df: pd.DataFrame, operand: str | float | int) -> pd.Series | float:
    if isinstance(operand, (int, float)):
        return operand
    return df[operand]


def gt(left: str | float, right: str | float) -> Rule:
    """left > right，操作数可以是字段名字符串或数字常数。"""
    def _rule(df: pd.DataFrame) -> pd.Series:
        return _resolve(df, left) > _resolve(df, right)
    return _tag_lookback(_rule, max(_parse_lookback(left), _parse_lookback(right)))


def lt(left: str | float, right: str | float) -> Rule:
    def _rule(df: pd.DataFrame) -> pd.Series:
        return _resolve(df, left) < _resolve(df, right)
    return _tag_lookback(_rule, max(_parse_lookback(left), _parse_lookback(right)))


def gte(left: str | float, right: str | float) -> Rule:
    def _rule(df: pd.DataFrame) -> pd.Series:
        return _resolve(df, left) >= _resolve(df, right)
    return _tag_lookback(_rule, max(_parse_lookback(left), _parse_lookback(right)))


def lte(left: str | float, right: str | float) -> Rule:
    def _rule(df: pd.DataFrame) -> pd.Series:
        return _resolve(df, left) <= _resolve(df, right)
    return _tag_lookback(_rule, max(_parse_lookback(left), _parse_lookback(right)))


def between(field: str, low: float, high: float) -> Rule:
    """字段值在 [low, high] 闭区间。"""
    def _rule(df: pd.DataFrame) -> pd.Series:
        s = df[field]
        return (s >= low) & (s <= high)
    return _tag_lookback(_rule, _parse_lookback(field))
```

- [ ] **Step 4: 运行测试通过**

Run: `uv run pytest tests/unit/test_screen_rules.py::TestCompareRules -v`
Expected: 6 passed

- [ ] **Step 5: Commit**

```bash
git add src/rquant/screen/rules.py tests/unit/test_screen_rules.py
git commit -m "feat(screen): add comparison rules (gt/lt/gte/lte/between)"
```

---

### Task 5: 均线 / 指标类积木

**Files:**
- Modify: `src/rquant/screen/rules.py`
- Modify: `tests/unit/test_screen_rules.py`

积木：`cross_above(fast, slow, offset=0)` / `cross_below(fast, slow, offset=0)` / `above_ma(period, offset=0)` / `rsi_oversold(period=14, threshold=30)` / `rsi_overbought(period=14, threshold=70)`。

**cross_above 语义**：`offset=0` 今日上穿 = 今日 fast > slow 且 昨日 fast ≤ slow。`min_lookback = offset + 1`。

- [ ] **Step 1: 写失败的测试**

追加到 `tests/unit/test_screen_rules.py`：
```python
from rquant.screen.rules import above_ma, cross_above, cross_below, rsi_overbought, rsi_oversold


class TestIndicatorRules:
    def test_cross_above_today(self) -> None:
        df = make_wide_frame(
            lookback=2,
            overrides={
                # 300001 今日上穿：今 MA5 > MA20，昨 MA5 <= MA20
                ("300001.SZ", "MA5[0]"): 12.0,
                ("300001.SZ", "MA20[0]"): 10.0,
                ("300001.SZ", "MA5[1]"): 9.0,
                ("300001.SZ", "MA20[1]"): 10.0,
                # 000001 未上穿：昨天已经在上方
                ("000001.SZ", "MA5[0]"): 12.0,
                ("000001.SZ", "MA20[0]"): 10.0,
                ("000001.SZ", "MA5[1]"): 11.0,
                ("000001.SZ", "MA20[1]"): 10.0,
            },
        )
        rule = cross_above("MA5", "MA20")
        mask = rule(df)
        assert mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert not mask.loc[df["ts_code"] == "000001.SZ"].iloc[0]
        assert rule.min_lookback == 1

    def test_cross_below_today(self) -> None:
        df = make_wide_frame(
            lookback=2,
            overrides={
                ("000001.SZ", "MA5[0]"): 9.0,
                ("000001.SZ", "MA20[0]"): 10.0,
                ("000001.SZ", "MA5[1]"): 11.0,
                ("000001.SZ", "MA20[1]"): 10.0,
            },
        )
        mask = cross_below("MA5", "MA20")(df)
        assert mask.loc[df["ts_code"] == "000001.SZ"].iloc[0]

    def test_above_ma(self) -> None:
        df = make_wide_frame(
            overrides={
                ("000001.SZ", "CLOSE[0]"): 15.0,
                ("000001.SZ", "MA20[0]"): 10.0,
                ("300001.SZ", "CLOSE[0]"): 8.0,
                ("300001.SZ", "MA20[0]"): 10.0,
            },
        )
        mask = above_ma(period=20)(df)
        assert mask.loc[df["ts_code"] == "000001.SZ"].iloc[0]
        assert not mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]

    def test_rsi_oversold(self) -> None:
        df = make_wide_frame(overrides={("000001.SZ", "RSI14[0]"): 25.0})
        mask = rsi_oversold()(df)
        assert mask.loc[df["ts_code"] == "000001.SZ"].iloc[0]
        assert not mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]

    def test_rsi_overbought(self) -> None:
        df = make_wide_frame(overrides={("000001.SZ", "RSI14[0]"): 75.0})
        mask = rsi_overbought()(df)
        assert mask.loc[df["ts_code"] == "000001.SZ"].iloc[0]
```

- [ ] **Step 2: 运行，确认失败**

Run: `uv run pytest tests/unit/test_screen_rules.py::TestIndicatorRules -v`
Expected: ImportError

- [ ] **Step 3: 实现积木**

追加到 `src/rquant/screen/rules.py`：
```python
def cross_above(fast: str, slow: str, offset: int = 0) -> Rule:
    """fast 均线在 offset 日上穿 slow 均线。"""
    def _rule(df: pd.DataFrame) -> pd.Series:
        f0 = df[f"{fast}[{offset}]"]
        s0 = df[f"{slow}[{offset}]"]
        f1 = df[f"{fast}[{offset + 1}]"]
        s1 = df[f"{slow}[{offset + 1}]"]
        return (f0 > s0) & (f1 <= s1)
    return _tag_lookback(_rule, offset + 1)


def cross_below(fast: str, slow: str, offset: int = 0) -> Rule:
    def _rule(df: pd.DataFrame) -> pd.Series:
        f0 = df[f"{fast}[{offset}]"]
        s0 = df[f"{slow}[{offset}]"]
        f1 = df[f"{fast}[{offset + 1}]"]
        s1 = df[f"{slow}[{offset + 1}]"]
        return (f0 < s0) & (f1 >= s1)
    return _tag_lookback(_rule, offset + 1)


def above_ma(period: int, offset: int = 0) -> Rule:
    """CLOSE 在 offset 日高于 MA{period}。"""
    def _rule(df: pd.DataFrame) -> pd.Series:
        return df[f"CLOSE[{offset}]"] > df[f"MA{period}[{offset}]"]
    return _tag_lookback(_rule, offset)


def rsi_oversold(period: int = 14, threshold: float = 30.0, offset: int = 0) -> Rule:
    """RSI 低于阈值（默认 30）。"""
    def _rule(df: pd.DataFrame) -> pd.Series:
        return df[f"RSI{period}[{offset}]"] < threshold
    return _tag_lookback(_rule, offset)


def rsi_overbought(period: int = 14, threshold: float = 70.0, offset: int = 0) -> Rule:
    def _rule(df: pd.DataFrame) -> pd.Series:
        return df[f"RSI{period}[{offset}]"] > threshold
    return _tag_lookback(_rule, offset)
```

- [ ] **Step 4: 运行测试通过**

Run: `uv run pytest tests/unit/test_screen_rules.py::TestIndicatorRules -v`
Expected: 5 passed

- [ ] **Step 5: Commit**

```bash
git add src/rquant/screen/rules.py tests/unit/test_screen_rules.py
git commit -m "feat(screen): add indicator rules (cross_above/cross_below/above_ma/rsi_oversold/rsi_overbought)"
```

---

### Task 6: 成交量积木 `volume_ratio_gte`

**Files:**
- Modify: `src/rquant/screen/rules.py`
- Modify: `tests/unit/test_screen_rules.py`

**语义**：`volume_ratio_gte(n, offset=0)` = 某日成交量 ≥ n × 前 5 日成交量均值。min_lookback = `offset + 5`。

- [ ] **Step 1: 写失败的测试**

追加到 `tests/unit/test_screen_rules.py`：
```python
from rquant.screen.rules import volume_ratio_gte


class TestVolumeRules:
    def test_volume_ratio_gte(self) -> None:
        df = make_wide_frame(
            lookback=5,
            overrides={
                # 今量 100 / 前 5 日均量 10 = 10 倍
                ("300001.SZ", "VOL[0]"): 100.0,
                ("300001.SZ", "VOL[1]"): 10.0,
                ("300001.SZ", "VOL[2]"): 10.0,
                ("300001.SZ", "VOL[3]"): 10.0,
                ("300001.SZ", "VOL[4]"): 10.0,
                ("300001.SZ", "VOL[5]"): 10.0,
                # 000001 今量 = 前 5 日均量，ratio = 1
                ("000001.SZ", "VOL[0]"): 10.0,
                ("000001.SZ", "VOL[1]"): 10.0,
                ("000001.SZ", "VOL[2]"): 10.0,
                ("000001.SZ", "VOL[3]"): 10.0,
                ("000001.SZ", "VOL[4]"): 10.0,
                ("000001.SZ", "VOL[5]"): 10.0,
            },
        )
        rule = volume_ratio_gte(2.0)
        mask = rule(df)
        assert mask.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert not mask.loc[df["ts_code"] == "000001.SZ"].iloc[0]
        assert rule.min_lookback == 5
```

- [ ] **Step 2: 运行，确认失败**

Run: `uv run pytest tests/unit/test_screen_rules.py::TestVolumeRules -v`
Expected: ImportError

- [ ] **Step 3: 实现积木**

追加到 `src/rquant/screen/rules.py`：
```python
def volume_ratio_gte(n: float, offset: int = 0, window: int = 5) -> Rule:
    """某日成交量 ≥ n × 前 {window} 日成交量均值。"""
    def _rule(df: pd.DataFrame) -> pd.Series:
        today = df[f"VOL[{offset}]"]
        prev_cols = [f"VOL[{offset + i}]" for i in range(1, window + 1)]
        mean_prev = df[prev_cols].mean(axis=1)
        return today >= n * mean_prev
    return _tag_lookback(_rule, offset + window)
```

- [ ] **Step 4: 运行测试通过**

Run: `uv run pytest tests/unit/test_screen_rules.py::TestVolumeRules -v`
Expected: 1 passed

- [ ] **Step 5: Commit**

```bash
git add src/rquant/screen/rules.py tests/unit/test_screen_rules.py
git commit -m "feat(screen): add volume_ratio_gte rule"
```

---

### Task 7: 入口 `screen()`（应用规则 + lookback 推断）

**Files:**
- Create: `tests/unit/test_screen_core.py`
- Modify: `src/rquant/screen/core.py`

`screen()` 先不碰 loader（用依赖注入），把"AND 合并 + 结果选列"这个纯逻辑先测通。

- [ ] **Step 1: 写失败的测试**

`tests/unit/test_screen_core.py`:
```python
"""screen() 主流程单测 —— 不依赖 DuckDB，注入假的 loader。"""

from unittest.mock import patch

import pandas as pd

from rquant.screen import screen
from rquant.screen.rules import gt, not_bj, not_st
from tests.fixtures.wide_frames import make_wide_frame


class TestScreenCore:
    def test_and_combine(self) -> None:
        df = make_wide_frame(
            overrides={
                ("000001.SZ", "is_st"): True,
                ("300001.SZ", "CLOSE[0]"): 15.0,
                ("300001.SZ", "PCT_CHG[0]"): 2.0,
                ("688001.SH", "CLOSE[0]"): 8.0,
                ("688001.SH", "PCT_CHG[0]"): 1.0,
            }
        )
        with patch("rquant.screen.core.load_universe", return_value=df):
            result = screen(
                trade_date="2026-04-15",
                rules=[not_st(), not_bj(), gt("CLOSE[0]", 10.0)],
            )
        assert list(result["ts_code"]) == ["300001.SZ"]
        assert set(result.columns) >= {"ts_code", "name", "CLOSE[0]", "PCT_CHG[0]"}

    def test_empty_result_returns_empty_df(self) -> None:
        df = make_wide_frame()
        with patch("rquant.screen.core.load_universe", return_value=df):
            result = screen(
                trade_date="2026-04-15",
                rules=[gt("CLOSE[0]", 9999.0)],
            )
        assert len(result) == 0
        assert list(result.columns)[:4] == ["ts_code", "name", "CLOSE[0]", "PCT_CHG[0]"]

    def test_include_columns(self) -> None:
        df = make_wide_frame(overrides={("300001.SZ", "MA20[0]"): 11.0})
        with patch("rquant.screen.core.load_universe", return_value=df):
            result = screen(
                trade_date="2026-04-15",
                rules=[not_st()],
                include_columns=["MA20[0]"],
            )
        assert "MA20[0]" in result.columns

    def test_lookback_auto_inferred_from_rules(self) -> None:
        df = make_wide_frame(lookback=3)
        from rquant.screen.rules import first_limit_up

        rules = [not_st(), first_limit_up(offset=2)]
        with patch("rquant.screen.core.load_universe") as mock_loader:
            mock_loader.return_value = df
            screen(trade_date="2026-04-15", rules=rules)
            _, kwargs = mock_loader.call_args
            assert kwargs.get("lookback", None) == 2 or mock_loader.call_args[0][1] == 2

    def test_explicit_lookback_overrides_inference(self) -> None:
        df = make_wide_frame(lookback=10)
        with patch("rquant.screen.core.load_universe") as mock_loader:
            mock_loader.return_value = df
            screen(trade_date="2026-04-15", rules=[not_st()], lookback=10)
            assert (
                mock_loader.call_args.kwargs.get("lookback") == 10
                or (len(mock_loader.call_args.args) >= 2 and mock_loader.call_args.args[1] == 10)
            )
```

- [ ] **Step 2: 运行，确认失败**

Run: `uv run pytest tests/unit/test_screen_core.py -v`
Expected: NotImplementedError / AttributeError — screen() 还没实现

- [ ] **Step 3: 实现 screen()**

替换 `src/rquant/screen/core.py` 内容：
```python
"""screen() 主流程：加载宽表 → 应用规则 → 返回结果。"""

from __future__ import annotations

from typing import Callable

import pandas as pd

from rquant.screen.loader import load_universe
from rquant.storage.duckdb import DuckDBStore

Rule = Callable[[pd.DataFrame], pd.Series]

BASE_COLUMNS = ["ts_code", "name", "CLOSE[0]", "PCT_CHG[0]"]


def _infer_lookback(rules: list[Rule]) -> int:
    return max((getattr(r, "min_lookback", 0) for r in rules), default=0)


def screen(
    trade_date: str,
    rules: list[Rule],
    lookback: int | None = None,
    include_columns: list[str] | None = None,
    store: DuckDBStore | None = None,
) -> pd.DataFrame:
    """筛选：给定 trade_date 和 rules，返回命中股票。

    - rules 列表内部按 AND 合并
    - lookback 默认按 rules 的 min_lookback 推断，最小 0
    - include_columns 控制结果附加列（base 列 ts_code/name/CLOSE[0]/PCT_CHG[0] 必出）
    """
    if lookback is None:
        lookback = _infer_lookback(rules)

    df = load_universe(trade_date, lookback=lookback, store=store)

    if df.empty:
        cols = list(BASE_COLUMNS)
        if include_columns:
            cols += [c for c in include_columns if c not in cols]
        return pd.DataFrame(columns=cols)

    mask = pd.Series(True, index=df.index)
    for rule in rules:
        mask &= rule(df)

    result = df.loc[mask].copy()

    cols = list(BASE_COLUMNS)
    if include_columns:
        cols += [c for c in include_columns if c not in cols]
    cols = [c for c in cols if c in result.columns]

    return result[cols].sort_values("ts_code").reset_index(drop=True)
```

- [ ] **Step 4: 运行测试通过**

Run: `uv run pytest tests/unit/test_screen_core.py -v`
Expected: 5 passed

- [ ] **Step 5: Commit**

```bash
git add src/rquant/screen/core.py tests/unit/test_screen_core.py
git commit -m "feat(screen): implement screen() with AND combine + auto lookback"
```

---

### Task 8: 宽表加载 `load_universe()`

**Files:**
- Modify: `src/rquant/screen/loader.py`
- Create: `tests/unit/test_screen_loader.py`

**核心逻辑**：
1. 从 `daily_bar` 找出 `trade_date` 当天所有有数据的 `ts_code`（= universe）
2. 从 `daily_bar` 取出 `trade_date` 往前 lookback+1 个交易日
3. 按日期排序，给每天打 offset 标签（0 = T 日，1 = T-1 日，...）
4. 用 pandas pivot 把 `(ts_code, offset)` 长表转成 `ts_code × <field>[offset]` 宽表
5. 左连 `stock_basic` 拿 name / market
6. 左连 `daily_indicator` 和 `daily_state`（按 ts_code + trade_date）
7. 从 T 日的 daily_state 取 `is_st` / `is_bj` / `board_type` 作为不分日属性

- [ ] **Step 1: 写失败的集成测试（用内存 DuckDB 塞数据）**

`tests/unit/test_screen_loader.py`:
```python
"""load_universe() 单测：用临时 DuckDB 实例塞数据。"""

from datetime import date

import pandas as pd
import pytest

from rquant.screen.loader import load_universe
from rquant.storage.duckdb import DuckDBStore


@pytest.fixture
def store(tmp_path) -> DuckDBStore:
    s = DuckDBStore(path=tmp_path / "test.duckdb")

    daily = pd.DataFrame([
        # 300001.SZ：3 天数据
        {"ts_code": "300001.SZ", "trade_date": date(2026, 4, 13), "open": 10.0,
         "high": 11.0, "low": 9.0, "close": 10.5, "pre_close": 10.0,
         "change": 0.5, "pct_chg": 5.0, "vol": 1000.0, "amount": 10500.0},
        {"ts_code": "300001.SZ", "trade_date": date(2026, 4, 14), "open": 10.5,
         "high": 12.0, "low": 10.0, "close": 11.0, "pre_close": 10.5,
         "change": 0.5, "pct_chg": 4.76, "vol": 1200.0, "amount": 13200.0},
        {"ts_code": "300001.SZ", "trade_date": date(2026, 4, 15), "open": 11.0,
         "high": 13.0, "low": 11.0, "close": 12.5, "pre_close": 11.0,
         "change": 1.5, "pct_chg": 13.64, "vol": 2000.0, "amount": 25000.0},
        # 000001.SZ：3 天数据
        {"ts_code": "000001.SZ", "trade_date": date(2026, 4, 13), "open": 20.0,
         "high": 21.0, "low": 19.0, "close": 20.5, "pre_close": 20.0,
         "change": 0.5, "pct_chg": 2.5, "vol": 500.0, "amount": 10250.0},
        {"ts_code": "000001.SZ", "trade_date": date(2026, 4, 14), "open": 20.5,
         "high": 21.5, "low": 20.0, "close": 21.0, "pre_close": 20.5,
         "change": 0.5, "pct_chg": 2.44, "vol": 600.0, "amount": 12600.0},
        {"ts_code": "000001.SZ", "trade_date": date(2026, 4, 15), "open": 21.0,
         "high": 22.0, "low": 20.5, "close": 21.5, "pre_close": 21.0,
         "change": 0.5, "pct_chg": 2.38, "vol": 700.0, "amount": 15050.0},
    ])
    s.upsert_daily(daily)

    basic = pd.DataFrame([
        {"ts_code": "300001.SZ", "symbol": "300001", "name": "特锐德",
         "area": "山东", "industry": "电气设备", "list_date": date(2009, 10, 30),
         "market": "创业板"},
        {"ts_code": "000001.SZ", "symbol": "000001", "name": "平安银行",
         "area": "深圳", "industry": "银行", "list_date": date(1991, 4, 3),
         "market": "主板"},
    ])
    s.upsert_stock_basic(basic)

    indicators = pd.DataFrame([
        {"ts_code": "300001.SZ", "trade_date": date(2026, 4, 15), "ma5": 11.5,
         "ma10": 11.0, "ma20": 10.5, "ma60": 10.0, "rsi6": 60.0, "rsi14": 55.0,
         "macd": 0.3, "macd_signal": 0.2, "macd_hist": 0.1,
         "kdj_k": 70.0, "kdj_d": 65.0, "kdj_j": 80.0},
        {"ts_code": "000001.SZ", "trade_date": date(2026, 4, 15), "ma5": 21.0,
         "ma10": 20.5, "ma20": 20.0, "ma60": 19.5, "rsi6": 50.0, "rsi14": 48.0,
         "macd": 0.1, "macd_signal": 0.1, "macd_hist": 0.0,
         "kdj_k": 55.0, "kdj_d": 52.0, "kdj_j": 60.0},
    ])
    s.upsert_indicators(indicators)

    state = pd.DataFrame([
        {"ts_code": "300001.SZ", "trade_date": date(2026, 4, 14),
         "is_st": False, "is_bj": False, "board_type": "gem",
         "limit_pct": 0.20, "limit_up_price": 12.60, "limit_down_price": 8.40,
         "is_limit_up": False, "is_limit_down": False, "is_first_limit_up": False,
         "is_yiziban": False, "consecutive_limit_ups": 0,
         "body_upper": 11.0, "body_lower": 10.5},
        {"ts_code": "300001.SZ", "trade_date": date(2026, 4, 15),
         "is_st": False, "is_bj": False, "board_type": "gem",
         "limit_pct": 0.20, "limit_up_price": 13.20, "limit_down_price": 8.80,
         "is_limit_up": True, "is_limit_down": False, "is_first_limit_up": True,
         "is_yiziban": False, "consecutive_limit_ups": 1,
         "body_upper": 12.5, "body_lower": 11.0},
        {"ts_code": "000001.SZ", "trade_date": date(2026, 4, 15),
         "is_st": False, "is_bj": False, "board_type": "main",
         "limit_pct": 0.10, "limit_up_price": 23.10, "limit_down_price": 18.90,
         "is_limit_up": False, "is_limit_down": False, "is_first_limit_up": False,
         "is_yiziban": False, "consecutive_limit_ups": 0,
         "body_upper": 21.5, "body_lower": 21.0},
    ])
    s.upsert_state(state)
    yield s
    s.close()


class TestLoadUniverse:
    def test_wide_frame_shape_and_columns(self, store: DuckDBStore) -> None:
        df = load_universe("2026-04-15", lookback=2, store=store)

        assert len(df) == 2
        assert set(df["ts_code"]) == {"300001.SZ", "000001.SZ"}
        assert "CLOSE[0]" in df.columns
        assert "CLOSE[1]" in df.columns
        assert "CLOSE[2]" in df.columns
        assert "MA20[0]" in df.columns
        assert "IS_FIRST_LIMIT_UP[0]" in df.columns
        assert "is_st" in df.columns
        assert "board_type" in df.columns
        assert "name" in df.columns

    def test_values_at_t0(self, store: DuckDBStore) -> None:
        df = load_universe("2026-04-15", lookback=2, store=store)
        row = df.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert row["CLOSE[0]"] == pytest.approx(12.5)
        assert row["HIGH[0]"] == pytest.approx(13.0)
        assert row["PRE_CLOSE[0]"] == pytest.approx(11.0)
        assert row["MA20[0]"] == pytest.approx(10.5)
        assert row["IS_FIRST_LIMIT_UP[0]"]
        assert row["board_type"] == "gem"
        assert row["name"] == "特锐德"

    def test_values_at_t1(self, store: DuckDBStore) -> None:
        df = load_universe("2026-04-15", lookback=2, store=store)
        row = df.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert row["CLOSE[1]"] == pytest.approx(11.0)
        assert row["HIGH[1]"] == pytest.approx(12.0)

    def test_lookback_zero_only_today(self, store: DuckDBStore) -> None:
        df = load_universe("2026-04-15", lookback=0, store=store)
        assert "CLOSE[0]" in df.columns
        assert "CLOSE[1]" not in df.columns

    def test_universe_is_t0_stocks_only(self, store: DuckDBStore) -> None:
        # 如果某只股票 T 日没数据（停牌/未上市），不应出现在结果里
        extra = pd.DataFrame([
            {"ts_code": "900001.SH", "trade_date": date(2026, 4, 13), "open": 5.0,
             "high": 5.5, "low": 4.5, "close": 5.0, "pre_close": 5.0,
             "change": 0.0, "pct_chg": 0.0, "vol": 100.0, "amount": 500.0}
        ])
        store.upsert_daily(extra)
        df = load_universe("2026-04-15", lookback=2, store=store)
        assert "900001.SH" not in set(df["ts_code"])
```

- [ ] **Step 2: 运行，确认失败**

Run: `uv run pytest tests/unit/test_screen_loader.py -v`
Expected: NotImplementedError

- [ ] **Step 3: 实现 load_universe**

替换 `src/rquant/screen/loader.py`：
```python
"""宽表加载：把 daily_bar/daily_indicator/daily_state/stock_basic 合并成
每行 1 只股票、价量字段带 [n] 后缀的宽表。"""

from __future__ import annotations

import pandas as pd

from rquant.storage.duckdb import DuckDBStore

PRICE_COLS_MAP = {
    "open": "OPEN",
    "high": "HIGH",
    "low": "LOW",
    "close": "CLOSE",
    "pre_close": "PRE_CLOSE",
    "vol": "VOL",
    "amount": "AMOUNT",
    "pct_chg": "PCT_CHG",
}

IND_COLS_MAP = {
    "ma5": "MA5", "ma10": "MA10", "ma20": "MA20", "ma60": "MA60",
    "rsi6": "RSI6", "rsi14": "RSI14",
    "macd": "MACD", "macd_signal": "MACD_SIGNAL", "macd_hist": "MACD_HIST",
    "kdj_k": "KDJ_K", "kdj_d": "KDJ_D", "kdj_j": "KDJ_J",
}

STATE_COLS_MAP = {
    "is_limit_up": "IS_LIMIT_UP",
    "is_limit_down": "IS_LIMIT_DOWN",
    "is_first_limit_up": "IS_FIRST_LIMIT_UP",
    "is_yiziban": "IS_YIZIBAN",
    "consecutive_limit_ups": "CONSECUTIVE_LIMIT_UPS",
}


def _resolve_trading_dates(
    store: DuckDBStore, trade_date: str, lookback: int
) -> list[str]:
    """返回 [T 日, T-1 日, ..., T-lookback 日] 的字符串日期列表。"""
    sql = """
    SELECT strftime(trade_date, '%Y-%m-%d') AS d
    FROM (
        SELECT DISTINCT trade_date FROM daily_bar
        WHERE trade_date <= ?
        ORDER BY trade_date DESC
        LIMIT ?
    )
    ORDER BY d DESC
    """
    rows = store._conn.execute(sql, [trade_date, lookback + 1]).fetchall()
    return [r[0] for r in rows]


def _wide_from_long(
    long_df: pd.DataFrame, rename_map: dict[str, str], date_to_offset: dict[str, int]
) -> pd.DataFrame:
    """把 (ts_code, trade_date_str, <field>...) 长表 pivot 成
    ts_code × <FIELD>[offset] 宽表。"""
    if long_df.empty:
        return pd.DataFrame(columns=["ts_code"])

    long_df = long_df.copy()
    long_df["offset"] = long_df["trade_date_str"].map(date_to_offset)
    long_df = long_df.dropna(subset=["offset"])
    long_df["offset"] = long_df["offset"].astype(int)

    frames: list[pd.DataFrame] = []
    for src, dst in rename_map.items():
        if src not in long_df.columns:
            continue
        p = long_df.pivot(index="ts_code", columns="offset", values=src)
        p.columns = [f"{dst}[{c}]" for c in p.columns]
        frames.append(p)

    if not frames:
        return pd.DataFrame({"ts_code": long_df["ts_code"].unique()})

    wide = pd.concat(frames, axis=1).reset_index()
    return wide


def load_universe(
    trade_date: str,
    lookback: int = 5,
    store: DuckDBStore | None = None,
) -> pd.DataFrame:
    owns_store = store is None
    store = store or DuckDBStore()

    try:
        dates = _resolve_trading_dates(store, trade_date, lookback)
        if not dates:
            return pd.DataFrame()
        date_to_offset = {d: i for i, d in enumerate(dates)}
        t0_date = dates[0]

        # universe：T 日有日线数据的所有股票
        universe_sql = """
        SELECT DISTINCT ts_code
        FROM daily_bar
        WHERE trade_date = ?
        """
        universe = store._conn.execute(universe_sql, [t0_date]).fetchdf()
        if universe.empty:
            return pd.DataFrame()

        in_universe = universe["ts_code"].tolist()
        placeholders = ",".join(["?"] * len(in_universe))

        # 日线 + 指标长表
        bar_sql = f"""
        SELECT ts_code,
               strftime(trade_date, '%Y-%m-%d') AS trade_date_str,
               {", ".join(PRICE_COLS_MAP.keys())}
        FROM daily_bar
        WHERE ts_code IN ({placeholders})
          AND trade_date IN ({",".join(["?"] * len(dates))})
        """
        bar_long = store._conn.execute(bar_sql, in_universe + dates).fetchdf()
        bar_wide = _wide_from_long(bar_long, PRICE_COLS_MAP, date_to_offset)

        ind_sql = f"""
        SELECT ts_code,
               strftime(trade_date, '%Y-%m-%d') AS trade_date_str,
               {", ".join(IND_COLS_MAP.keys())}
        FROM daily_indicator
        WHERE ts_code IN ({placeholders})
          AND trade_date IN ({",".join(["?"] * len(dates))})
        """
        ind_long = store._conn.execute(ind_sql, in_universe + dates).fetchdf()
        ind_wide = _wide_from_long(ind_long, IND_COLS_MAP, date_to_offset)

        state_sql = f"""
        SELECT ts_code,
               strftime(trade_date, '%Y-%m-%d') AS trade_date_str,
               {", ".join(STATE_COLS_MAP.keys())},
               is_st, is_bj, board_type
        FROM daily_state
        WHERE ts_code IN ({placeholders})
          AND trade_date IN ({",".join(["?"] * len(dates))})
        """
        state_long = store._conn.execute(state_sql, in_universe + dates).fetchdf()
        state_wide = _wide_from_long(state_long, STATE_COLS_MAP, date_to_offset)

        # 不分日属性：取 T 日的 is_st / is_bj / board_type
        state_t0 = state_long[state_long["trade_date_str"] == t0_date][
            ["ts_code", "is_st", "is_bj", "board_type"]
        ].drop_duplicates(subset=["ts_code"])

        # stock_basic 拿 name
        basic = store._conn.execute(
            f"SELECT ts_code, name FROM stock_basic WHERE ts_code IN ({placeholders})",
            in_universe,
        ).fetchdf()

        # 合并所有
        out = universe.merge(basic, on="ts_code", how="left")
        out = out.merge(state_t0, on="ts_code", how="left")
        for wide in (bar_wide, ind_wide, state_wide):
            if not wide.empty:
                out = out.merge(wide, on="ts_code", how="left")

        # 默认值填充
        if "is_st" in out.columns:
            out["is_st"] = out["is_st"].fillna(False).astype(bool)
        if "is_bj" in out.columns:
            out["is_bj"] = out["is_bj"].fillna(False).astype(bool)

        return out
    finally:
        if owns_store:
            store.close()
```

- [ ] **Step 4: 运行测试通过**

Run: `uv run pytest tests/unit/test_screen_loader.py -v`
Expected: 5 passed

- [ ] **Step 5: 全部单测通过**

Run: `uv run pytest tests/unit -v`
Expected: 所有单测（原 65 个 + 本次新增的 ~24 个 rules + 5 screen_core + 5 loader）全通过

- [ ] **Step 6: Commit**

```bash
git add src/rquant/screen/loader.py tests/unit/test_screen_loader.py
git commit -m "feat(screen): implement load_universe() — build wide frame from duckdb tables"
```

---

### Task 9: 端到端集成测试（用户原始场景）

**Files:**
- Create: `tests/integration/test_screen_e2e.py`

验证：用用户给的真实场景"非 ST + 非北交所 + 昨首板 + 今未涨停 + 今高>昨收"能跑通全链路。

- [ ] **Step 1: 写集成测试**

`tests/integration/test_screen_e2e.py`:
```python
"""screen() 端到端测试：复刻用户原始场景。"""

from datetime import date

import pandas as pd
import pytest

from rquant.screen import screen
from rquant.screen.rules import first_limit_up, gt, not_bj, not_limit_up, not_st
from rquant.storage.duckdb import DuckDBStore


@pytest.mark.integration
class TestUserScenario:
    """非 ST + 非北交所 + 昨首板 + 今未涨停 + 今高>昨收。"""

    @pytest.fixture
    def store(self, tmp_path) -> DuckDBStore:
        s = DuckDBStore(path=tmp_path / "e2e.duckdb")

        # 三只股票，T-1 = 2026-04-14（周二）、T = 2026-04-15（周三）
        #   300001.SZ：昨首板、今未涨停、今高>昨收 → 命中
        #   000001.SZ：昨首板、今未涨停、今高<昨收 → 不命中
        #   833001.BJ：和 300001 同形态，但北交所 → 不命中
        daily_rows = []
        state_rows = []
        basic_rows = []
        for code, nm, is_bj, board, p_prev, o_today, h_today, l_today, c_today, limit_up_yesterday in [
            ("300001.SZ", "特锐德", False, "gem", 10.0, 11.0, 13.0, 11.0, 12.0, True),
            ("000001.SZ", "平安银行", False, "main", 20.0, 20.0, 20.5, 19.5, 19.8, True),
            ("833001.BJ", "北交所", True, "bj", 5.0, 5.5, 7.0, 5.2, 6.3, True),
        ]:
            daily_rows.extend([
                {"ts_code": code, "trade_date": date(2026, 4, 14), "open": p_prev,
                 "high": p_prev * 1.1, "low": p_prev, "close": p_prev * 1.1,
                 "pre_close": p_prev, "change": p_prev * 0.1, "pct_chg": 10.0,
                 "vol": 1000.0, "amount": 10000.0},
                {"ts_code": code, "trade_date": date(2026, 4, 15), "open": o_today,
                 "high": h_today, "low": l_today, "close": c_today,
                 "pre_close": p_prev * 1.1, "change": c_today - p_prev * 1.1,
                 "pct_chg": (c_today - p_prev * 1.1) / (p_prev * 1.1) * 100,
                 "vol": 1200.0, "amount": 12000.0},
            ])
            basic_rows.append({
                "ts_code": code, "symbol": code.split(".")[0], "name": nm,
                "area": "X", "industry": "Y", "list_date": date(2020, 1, 1),
                "market": board,
            })
            state_rows.extend([
                {"ts_code": code, "trade_date": date(2026, 4, 14),
                 "is_st": False, "is_bj": is_bj, "board_type": board,
                 "limit_pct": 0.10, "limit_up_price": p_prev * 1.1,
                 "limit_down_price": p_prev * 0.9,
                 "is_limit_up": True, "is_limit_down": False,
                 "is_first_limit_up": limit_up_yesterday, "is_yiziban": False,
                 "consecutive_limit_ups": 1,
                 "body_upper": p_prev * 1.1, "body_lower": p_prev},
                {"ts_code": code, "trade_date": date(2026, 4, 15),
                 "is_st": False, "is_bj": is_bj, "board_type": board,
                 "limit_pct": 0.10, "limit_up_price": p_prev * 1.21,
                 "limit_down_price": p_prev * 0.99,
                 "is_limit_up": False, "is_limit_down": False,
                 "is_first_limit_up": False, "is_yiziban": False,
                 "consecutive_limit_ups": 0,
                 "body_upper": max(o_today, c_today),
                 "body_lower": min(o_today, c_today)},
            ])
        s.upsert_daily(pd.DataFrame(daily_rows))
        s.upsert_stock_basic(pd.DataFrame(basic_rows))
        s.upsert_state(pd.DataFrame(state_rows))
        yield s
        s.close()

    def test_user_scenario(self, store: DuckDBStore) -> None:
        result = screen(
            trade_date="2026-04-15",
            rules=[
                not_st(),
                not_bj(),
                first_limit_up(offset=1),
                not_limit_up(offset=0),
                gt("HIGH[0]", "CLOSE[1]"),
            ],
            store=store,
        )
        assert list(result["ts_code"]) == ["300001.SZ"]
        assert result.loc[0, "name"] == "特锐德"
```

- [ ] **Step 2: 运行**

Run: `uv run pytest tests/integration/test_screen_e2e.py -v -m integration`
Expected: 1 passed

- [ ] **Step 3: 全量测试通过**

Run: `uv run pytest -v -m "not network"`
Expected: 全绿

- [ ] **Step 4: Commit**

```bash
git add tests/integration/test_screen_e2e.py
git commit -m "test(screen): add end-to-end scenario (yesterday first limit-up + today no limit + high>prev close)"
```

---

### Task 10: 真实数据冒烟 + CHANGELOG + 合并准备

**Files:**
- Create: `scripts/smoke_screen.py`
- Modify: `CHANGELOG.md`
- Modify: `src/rquant/screen/__init__.py`（暴露所有积木）

- [ ] **Step 1: 暴露积木到 package 顶层**

替换 `src/rquant/screen/__init__.py`：
```python
"""筛选引擎：积木函数 + screen() 入口。"""

from rquant.screen.core import screen
from rquant.screen.loader import load_universe
from rquant.screen.rules import (
    # 属性类
    not_st, not_bj, board_in,
    # 涨跌停 / 连板
    limit_up, not_limit_up, first_limit_up, yiziban, limit_down,
    consecutive_ups_gte,
    # 比较
    gt, lt, gte, lte, between,
    # 指标
    cross_above, cross_below, above_ma, rsi_oversold, rsi_overbought,
    # 成交量
    volume_ratio_gte,
)

__all__ = [
    "screen", "load_universe",
    "not_st", "not_bj", "board_in",
    "limit_up", "not_limit_up", "first_limit_up", "yiziban", "limit_down",
    "consecutive_ups_gte",
    "gt", "lt", "gte", "lte", "between",
    "cross_above", "cross_below", "above_ma", "rsi_oversold", "rsi_overbought",
    "volume_ratio_gte",
]
```

- [ ] **Step 2: 写冒烟脚本跑真实数据**

`scripts/smoke_screen.py`:
```python
"""Week 3b 冒烟脚本：在本地 DuckDB 的真实数据上跑用户原始场景，肉眼检查结果。

运行前确保 data/warehouse/rquant.duckdb 已有 2024-10 前后的赛力斯连板数据
（Week 3a 冒烟时已经灌过）。
"""

from __future__ import annotations

import sys

from rquant.screen import (
    first_limit_up,
    gt,
    not_bj,
    not_limit_up,
    not_st,
    screen,
)


def main(trade_date: str) -> None:
    result = screen(
        trade_date=trade_date,
        rules=[
            not_st(),
            not_bj(),
            first_limit_up(offset=1),
            not_limit_up(offset=0),
            gt("HIGH[0]", "CLOSE[1]"),
        ],
        include_columns=["MA20[0]", "CONSECUTIVE_LIMIT_UPS[1]"],
    )
    print(f"[{trade_date}] 命中 {len(result)} 只股票")
    print(result.to_string(index=False))


if __name__ == "__main__":
    date = sys.argv[1] if len(sys.argv) > 1 else "2024-11-05"
    main(date)
```

- [ ] **Step 3: 冒烟运行 + 肉眼核验**

Run: `uv run python scripts/smoke_screen.py 2024-11-05`

Expected：
- 打印若干只"昨首板+今未涨停+今高>昨收"的股票
- 数量 > 0（2024 年 10-11 月是打板高峰期）
- 如果结果为空，先运行 `uv run python -m rquant.cli status` 确认本地数据覆盖了 2024-10 前后

- [ ] **Step 4: 更新 CHANGELOG**

在 `CHANGELOG.md` 的 `[Unreleased]` section 前面，插入新版：
```markdown
## [v0.3.0] — 2026-04-16 — Week 3b: 筛选规则引擎

Week 3b 在 daily_state + daily_indicator 基础上做多条件组合筛选。原子条件"积木"函数库，命名对齐通达信/MyTT 风格（`CLOSE[0]` / `MA20[0]` / `IS_LIMIT_UP[1]`），为 Week 8 通达信代码支持铺路。

### Added
- `rquant.screen` package：
  - `load_universe(trade_date, lookback)`：从 DuckDB 加载全市场宽表（每行 1 只股票，字段 `CLOSE[n]` / `MA20[n]` / `IS_LIMIT_UP[n]` 等）
  - 积木函数库：属性（not_st / not_bj / board_in）、涨跌停（limit_up / first_limit_up / yiziban / consecutive_ups_gte / limit_down / not_limit_up）、比较（gt / lt / gte / lte / between）、指标（cross_above / cross_below / above_ma / rsi_oversold / rsi_overbought）、成交量（volume_ratio_gte）
  - `screen(trade_date, rules)`：AND 组合 + 自动 lookback 推断 + 结果 DataFrame 返回
- `scripts/smoke_screen.py`：跑用户原始场景的冒烟脚本

### Verified
- 用户原始场景「非 ST + 非北交所 + 昨首板 + 今未涨停 + 今高>昨收」在集成测试 + 真实 2024-11-05 数据上均返回合理结果
- 单测：新增 ~30 条（属性 6 + 涨跌停 7 + 比较 6 + 指标 5 + 成交量 1 + screen 5 + loader 5），累计约 95 个全绿
```

删除 `[Unreleased]` 下的空条目（保留结构）。

- [ ] **Step 5: 全量测试 + lint**

Run:
```bash
uv run pytest -v -m "not network"
uv run ruff check src tests
```
Expected: 全绿、0 lint 问题

- [ ] **Step 6: Commit + tag**

```bash
git add src/rquant/screen/__init__.py scripts/smoke_screen.py CHANGELOG.md
git commit -m "chore(week3b): expose public rules api + smoke script + changelog"
git tag -a v0.3.0 -m "Week 3b: 筛选规则引擎（积木函数库 + screen 入口）"
```

- [ ] **Step 7: 告诉用户如何验收**

给用户这段话：

> Week 3b 开发完成，合 main 前请验收：
>
> - 分支：`feat/week3b-screening`
> - 工作目录：`/Users/roxor/brain/30-projects/rQuant-week3b`
>
> 验收命令：
> ```
> cd /Users/roxor/brain/30-projects/rQuant-week3b
> uv run python scripts/smoke_screen.py 2024-11-05
> ```
>
> 你看到的应该是一张表格，列是 `ts_code / name / CLOSE[0] / PCT_CHG[0] / MA20[0] / CONSECUTIVE_LIMIT_UPS[1]`，列出"昨首板+今未涨停+今高>昨收"的股票。
>
> 验收通过 → 我合 `feat/week3b-screening` 到 main、打 tag `v0.3.0`、移除 worktree。

---

## Self-Review

**Spec coverage check**（对照 design doc 第 5 节积木清单）：
- ✅ 5.1 属性：not_st / not_bj / board_in → Task 2
- ✅ 5.2 涨跌停：limit_up / not_limit_up / first_limit_up / yiziban / consecutive_ups_gte / limit_down → Task 3
- ✅ 5.3 比较：gt / lt / gte / lte / between → Task 4
- ✅ 5.4 指标：cross_above / cross_below / above_ma / rsi_oversold / rsi_overbought → Task 5
- ✅ 5.5 成交量：volume_ratio_gte → Task 6
- ✅ 5.6 入口 screen() → Task 7
- ✅ 第 4 节宽表加载 → Task 8
- ✅ 第 6 节测试策略（用户原始场景端到端） → Task 9
- ✅ 第 10 节 DoD（CHANGELOG + tag + 实际数据验证） → Task 10

**Placeholder scan**：无 TBD / TODO / "implement later"。所有步骤都有可执行代码或具体命令。

**Type/命名一致性**：
- 宽表列命名 `CLOSE[0]` 等大写格式在 loader (Task 8)、rules (Task 2-6)、core (Task 7)、测试（所有）中一致
- `min_lookback` 属性在 rules 所有积木上统一挂载，core 通过 `_infer_lookback` 读取
- `Rule` type alias 在 `rules.py` 和 `core.py` 各定义一次（都是 `Callable[[pd.DataFrame], pd.Series]`），语义一致

---

**Plan complete and saved to `/Users/roxor/brain/30-projects/rQuant-week3b/docs/superpowers/plans/2026-04-16-week3b-screening.md`.**

Two execution options:

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks.

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

**Which approach?**
